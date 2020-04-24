(ns tech.ml.dataset.impl.dataset
  (:require [tech.ml.dataset.column :as ds-col]
            [tech.ml.protocols.dataset :as ds-proto]
            [tech.ml.dataset.impl.column :as col-impl]
            [tech.ml.dataset.parse :as ds-parse]
            [tech.ml.dataset.print :as ds-print]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.protocols :as dtype-proto]
            [clojure.set :as c-set])
  (:import [java.io Writer]
           [clojure.lang IPersistentMap IObj IFn Counted Indexed]
           [java.util Map List]))


(declare new-dataset)


(deftype Dataset [column-names
                  colmap
                  ^IPersistentMap metadata]
  ds-proto/PColumnarDataset
  (dataset-name [dataset] (:name metadata))
  (set-dataset-name [dataset new-name]
    (Dataset.
     column-names
     colmap
     (assoc metadata :name new-name)))
  (maybe-column [dataset column-name]
    (get colmap column-name))

  (metadata [dataset] metadata)
  (set-metadata [dataset meta-map]
    (Dataset. column-names colmap
              (col-impl/->persistent-map meta-map)))

  (columns [dataset] (mapv (partial get colmap) column-names))

  (add-column [dataset col]
    (let [existing-names (set column-names)
          new-col-name (ds-col/column-name col)]
      (when (existing-names new-col-name)
        (throw (ex-info (format "Column of same name (%s) already exists in columns"
                                new-col-name)
                        {:existing-columns existing-names
                         :column-name new-col-name})))

      (new-dataset
       (ds-proto/dataset-name dataset)
       metadata
       (concat (ds-proto/columns dataset) [col]))))

  (remove-column [dataset col-name]
    (->> (ds-proto/columns dataset)
         (remove #(= (ds-col/column-name %)
                     col-name))
         (new-dataset (ds-proto/dataset-name dataset) metadata)))

  (update-column [dataset col-name col-fn]
    (when-not (contains? colmap col-name)
      (throw (ex-info (format "Failed to find column %s" col-name)
                      {:col-name col-name
                       :col-names (keys colmap)})))
    (let [col (get colmap col-name)
          new-col-data (col-fn col)]
      (Dataset.
       column-names
       (assoc colmap col-name
              (if (ds-col/is-column? new-col-data)
                (ds-col/set-name new-col-data col-name)
                (ds-col/new-column (ds-col/column-name col) new-col-data)))
       metadata)))

  (add-or-update-column [dataset col-name new-col-data]
    (let [col-data (if (ds-col/is-column? new-col-data)
                     (ds-col/set-name new-col-data col-name)
                     (ds-col/new-column col-name new-col-data))]
      (if (contains? colmap col-name)
        (ds-proto/update-column dataset col-name (constantly col-data))
        (ds-proto/add-column dataset col-data))))

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
                    index-seq)]
      (->> column-name-seq
           (map (fn [col-name]
                  (let [col (ds-proto/column dataset col-name)]
                    (if indexes
                      (ds-col/select col indexes)
                      col))))
           (new-dataset (ds-proto/dataset-name dataset) metadata))))


  (supported-column-stats [dataset]
    (ds-col/supported-stats (first (vals colmap))))


  (from-prototype [dataset dataset-name column-seq]
    (new-dataset dataset-name column-seq))


  dtype-proto/PShape
  (shape [m]
    [(count column-names)
     (if-let [first-col (first (vals colmap))]
       (dtype/ecount first-col)
       0)])

  dtype-proto/PCopyRawData
  (copy-raw->item! [raw-data ary-target target-offset options]
    (dtype-proto/copy-raw->item! (ds-proto/columns raw-data) ary-target
                                 target-offset options))
  dtype-proto/PClone
  (clone [item]
    (new-dataset (ds-proto/dataset-name item)
                 metadata
                 (mapv dtype/clone (ds-proto/columns item))))
  Counted
  (count [this] (count column-names))

  IFn
  (invoke [item col-name]
    (ds-proto/column item col-name))
  (invoke [item col-name new-col]
    (ds-proto/add-column item (ds-col/set-name new-col col-name)))
  (applyTo [this arg-seq]
    (case (count arg-seq)
      1 (.invoke this (first arg-seq))
      2 (.invoke this (first arg-seq) (second arg-seq))))

  IObj
  (meta [this] metadata)
  (withMeta [this metadata] (Dataset. column-names colmap metadata))

  Iterable
  (iterator [item]
    (->> (ds-proto/columns item)
         (.iterator)))

  Object
  (toString [item]
    (format "%s %s:\n%s"
            (ds-proto/dataset-name item)
            ;;make row major shape to avoid confusion
            (vec (reverse (dtype/shape item)))
            (ds-print/print-dataset item))))


(defn new-dataset
  "Create a new dataset from a sequence of columns.  Data will be converted
  into columns using ds-col/ensure-column-seq.  If the column seq is simply a
  collection of vectors, for instance, columns will be named ordinally.
  options map -
    :dataset-name - Name of the dataset.  Defaults to \"_unnamed\".
    :key-fn - Key function used on all column names before insertion into dataset.

  The return value fulfills the dataset protocols."
  ([options ds-metadata column-seq]
   (let [column-seq (ds-col/ensure-column-seq column-seq)
         ;;Options was dataset-name so have to keep that pathway going.
         dataset-name (if (map? options)
                        (:dataset-name options)
                        options)
         column-seq (if (and (map? options)
                             (:key-fn options))
                      (let [key-fn (:key-fn options)]
                        (->> column-seq
                             (map #(ds-col/set-name
                                    %
                                    (key-fn (ds-col/column-name %))))))
                      column-seq)]
     (Dataset. (mapv ds-col/column-name column-seq)
               (->> column-seq
                    (map (juxt ds-col/column-name identity))
                    (into {}))
               (assoc (col-impl/->persistent-map ds-metadata)
                      :name
                      dataset-name))))
  ([options column-seq]
   (new-dataset options {} column-seq))
  ([column-seq]
   (new-dataset {} {} column-seq)))


(defmethod print-method Dataset
  [dataset w]
  (.write ^Writer w (.toString dataset)))


(defn item-val->string
  [item-val item-dtype item-name]
  (cond
    (string? item-val) item-val
    (keyword? item-val) item-val
    (symbol? item-val) item-val
    :else
    (str item-val)))


(defn parse-dataset
  ([input options]
   (->> (ds-parse/csv->columns input options)
        (new-dataset options {})))
  ([input]
   (parse-dataset input {})))
