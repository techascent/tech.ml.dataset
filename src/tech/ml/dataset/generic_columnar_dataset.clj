(ns tech.ml.dataset.generic-columnar-dataset
  (:require [tech.ml.dataset.column :as ds-col]
            [tech.ml.dataset :as ds]
            [tech.ml.protocols.dataset :as ds-proto]
            [tech.v2.datatype.base :as dtype-base]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.protocols :as dtype-proto]
            [clojure.set :as c-set])
  (:import [java.io Writer]))


(declare make-dataset)


(deftype GenericColumnarDataset [table-name
                                 column-names
                                 colmap
                                 metadata]
  ds-proto/PColumnarDataset
  (dataset-name [dataset] table-name)
  (set-dataset-name [dataset new-name]
    (GenericColumnarDataset.
     new-name
     column-names
     colmap
     metadata))
  (maybe-column [dataset column-name]
    (get colmap column-name))

  (metadata [dataset] metadata)
  (set-metadata [dataset meta-map]
    (GenericColumnarDataset. table-name column-names colmap
                             meta-map))

  (columns [dataset] (mapv (partial get colmap) column-names))

  (add-column [dataset col]
    (let [existing-names (set column-names)
          new-col-name (ds-col/column-name col)]
      (when-let [existing (existing-names new-col-name)]
        (throw (ex-info (format "Column of same name (%s) already exists in columns"
                                new-col-name)
                        {:existing-columns existing-names
                         :column-name new-col-name})))

      (make-dataset
       table-name
       (concat (ds-proto/columns dataset) [col])
       metadata)))

  (remove-column [dataset col-name]
    (make-dataset table-name
                  (->> (ds-proto/columns dataset)
                       (remove #(= (ds-col/column-name %)
                                   col-name)))
                  metadata))

  (update-column [dataset col-name col-fn]
    (when-not (contains? colmap col-name)
      (throw (ex-info (format "Failed to find column %s" col-name)
                      {:col-name col-name
                       :col-names (keys colmap)})))
    (let [col (get colmap col-name)
          new-col-data (col-fn col)]
      (GenericColumnarDataset.
       table-name
       column-names
       (assoc colmap col-name
              (if (ds-col/is-column? new-col-data)
                (ds-col/set-name new-col-data col-name)
                (ds-col/new-column col (dtype/get-datatype new-col-data)
                                   new-col-data {:name (ds-col/column-name col)})))
       metadata)))

  (add-or-update-column [dataset column]
    (let [col-name (ds-col/column-name column)]
      (if (contains? colmap col-name)
        (ds/update-column dataset col-name (constantly column))
        (ds/add-column dataset column))))

  (select [dataset column-name-seq index-seq]
    (let [all-names column-names
          all-name-set (set all-names)
          column-name-seq (if (= :all column-name-seq)
                            all-names
                            column-name-seq)
          name-set (set column-name-seq)
          _ (when-let [missing (seq (c-set/difference name-set all-name-set))]
              (throw (ex-info (format "Invalid/missing column names: %s" missing)
                              {:all-columns all-name-set
                               :selection column-name-seq})))
          _ (when-not (= (count name-set)
                         (count column-name-seq))
              (throw (ex-info "Duplicate column names detected"
                              {:selection column-name-seq})))
          indexes (if (= :all index-seq)
                    nil
                    (int-array index-seq))]
      (make-dataset
       table-name
       (->> column-name-seq
            (map (fn [col-name]
                   (let [col (ds/column dataset col-name)]
                     (if indexes
                       (ds-col/select col indexes)
                       col))))
            vec)
       metadata)))

  (index-value-seq [dataset]
    (let [col-value-seq (->> (ds-proto/columns dataset)
                             (mapv (fn [col]
                                     (ds-col/column-values col))))]
      (->> (apply map vector col-value-seq)
           (map-indexed vector))))

  (supported-column-stats [dataset]
    (ds-col/supported-stats (first (vals colmap))))


  (from-prototype [dataset table-name column-seq]
    (make-dataset table-name column-seq {}))


  dtype-proto/PShape
  (shape [m]
    [(count column-names)
     (if-let [first-col (first (vals colmap))]
       (dtype/ecount first-col)
       0)])

  dtype-proto/PCopyRawData
  (copy-raw->item! [raw-data ary-target target-offset options]
    (dtype-proto/copy-raw->item! (ds/columns raw-data) ary-target
                                 target-offset options))

  Object
  (toString [item]
    (format "%s %s:\n%s"
            (ds-proto/dataset-name item)
            ;;make row major shape to avoid confusion
            (vec (reverse (dtype/shape item)))
            (ds/dataset->string item))))


(defn make-dataset
  [table-name column-seq ds-metadata]
  (GenericColumnarDataset. table-name
                           (map ds-col/column-name column-seq)
                           (->> column-seq
                                (map (juxt ds-col/column-name identity))
                                (into {}))
                           ds-metadata))


(defmethod print-method GenericColumnarDataset
  [dataset w]
  (.write ^Writer w (.toString dataset)))
