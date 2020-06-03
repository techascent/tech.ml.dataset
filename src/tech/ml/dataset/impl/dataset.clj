(ns tech.ml.dataset.impl.dataset
  (:require [tech.ml.dataset.column :as ds-col]
            [tech.ml.protocols.dataset :as ds-proto]
            [tech.ml.dataset.impl.column :as col-impl]
            [tech.ml.dataset.parse :as ds-parse]
            [tech.ml.dataset.print :as ds-print]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.bitmap :as bitmap]
            [clojure.set :as c-set])
  (:import [java.io Writer]
           [clojure.lang IPersistentMap IObj IFn Counted Indexed]
           [java.util Map List]))


(declare new-dataset)


(deftype Dataset [^List columns
                  colmap
                  ^IPersistentMap metadata]
  ds-proto/PColumnarDataset
  (dataset-name [dataset] (:name metadata))
  (set-dataset-name [dataset new-name]
    (Dataset. columns colmap
     (assoc metadata :name new-name)))
  (maybe-column [dataset column-name]
    (when-let [idx (get colmap column-name)]
      (.get columns (int idx))))

  (metadata [dataset] metadata)
  (set-metadata [dataset meta-map]
    (Dataset. columns colmap
              (col-impl/->persistent-map meta-map)))

  (columns [dataset] columns)

  (add-column [dataset col]
    (let [existing-names (set (map ds-col/column-name columns))
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
    (->> columns
         (remove #(= (ds-col/column-name %) col-name))
         (new-dataset (ds-proto/dataset-name dataset) metadata)))

  (update-column [dataset col-name col-fn]
    (when-not (contains? colmap col-name)
      (throw (ex-info (format "Failed to find column %s" col-name)
                      {:col-name col-name
                       :col-names (keys colmap)})))
    (let [col-idx (get colmap col-name)
          col (.get columns (int col-idx))
          new-col-data (col-fn col)]
      (Dataset.
       (assoc columns col-idx
              (if (ds-col/is-column? new-col-data)
                (ds-col/set-name new-col-data col-name)
                (ds-col/new-column (ds-col/column-name col) new-col-data)))
       colmap
       metadata)))

  (add-or-update-column [dataset col-name new-col-data]
    (let [col-data (if (ds-col/is-column? new-col-data)
                     (ds-col/set-name new-col-data col-name)
                     (ds-col/new-column col-name new-col-data))]
      (if (contains? colmap col-name)
        (ds-proto/update-column dataset col-name (constantly col-data))
        (ds-proto/add-column dataset col-data))))

  (select [dataset column-name-seq-or-map index-seq]
    ;;Conversion to a reader is expensive in some cases so do it here
    ;;to avoid each column doing it.
    (let [map-selector? (instance? Map column-name-seq-or-map)
          indexes (if (= :all index-seq)
                    nil
                    (if-let [bmp (dtype/as-roaring-bitmap index-seq)]
                      (dtype/->reader
                       (bitmap/bitmap->efficient-random-access-reader
                        bmp))
                      (if (dtype/reader? index-seq)
                        (dtype/->reader index-seq)
                        (dtype/->reader (dtype/make-container
                                         :java-array :int32
                                         index-seq)))))
          columns
          (cond
            (= :all column-name-seq-or-map)
            columns
            map-selector?
            (->> column-name-seq-or-map
                 (map (fn [[old-name new-name]]
                        (if-let [col-idx (get colmap old-name)]
                          (let [col (.get columns (unchecked-int col-idx))]
                            (ds-col/set-name col new-name))
                          (throw (Exception.
                                  (format "Failed to find column %s" old-name)))))))
            :else
            (->> column-name-seq-or-map
                 (map (fn [colname]
                        (if-let [col-idx (get colmap colname)]
                          (.get columns (unchecked-int col-idx))
                          (throw (Exception.
                                  (format "Failed to find column %s" colname))))))))]
      (->> columns
           ;;select may be slow if we have to recalculate missing values.
           (map (fn [col]
                  (if indexes
                    (ds-col/select col indexes)
                    col)))
           (new-dataset (ds-proto/dataset-name dataset) metadata))))

  (select-columns-by-index [dataset num-seq]
    (let [col-indexes (int-array (distinct num-seq))]
      (when-not (== (count col-indexes) (count num-seq))
        (throw (Exception. (format "Duplicate column selection detected: %s"
                                   num-seq))))
      (->> col-indexes
           (map (fn [^long idx]
                  (.get columns idx)))
           (new-dataset (ds-proto/dataset-name dataset) metadata))))


  (supported-column-stats [dataset]
    (ds-col/supported-stats (first columns)))


  (from-prototype [dataset dataset-name column-seq]
    (new-dataset dataset-name column-seq))


  dtype-proto/PShape
  (shape [m]
    [(count columns)
     (if-let [first-col (first columns)]
       (dtype/ecount first-col)
       0)])

  dtype-proto/PCopyRawData
  (copy-raw->item! [raw-data ary-target target-offset options]
    (dtype-proto/copy-raw->item! columns ary-target target-offset options))
  dtype-proto/PClone
  (clone [item]
    (new-dataset (ds-proto/dataset-name item)
                 metadata
                 (mapv dtype/clone columns)))
  Counted
  (count [this] (count columns))

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
  (withMeta [this metadata] (Dataset. columns colmap metadata))

  Iterable
  (iterator [item]
    (->> (ds-proto/columns item)
         (.iterator)))
  Object
  (toString [item]
    (ds-print/dataset->str item)))


(defn new-dataset
  "Create a new dataset from a sequence of columns.  Data will be converted
  into columns using ds-col/ensure-column-seq.  If the column seq is simply a
  collection of vectors, for instance, columns will be named ordinally.
  options map -
    :dataset-name - Name of the dataset.  Defaults to \"_unnamed\".
    :key-fn - Key function used on all column names before insertion into dataset.

  The return value fulfills the dataset protocols."
  ([options ds-metadata column-seq]
   (let [column-seq (->> (ds-col/ensure-column-seq column-seq)
                         (map-indexed (fn [idx column]
                                        (let [cname (ds-col/column-name
                                                     column)]
                                          (if (and (string? cname)
                                                   (empty? cname))
                                            (ds-col/set-name column idx)
                                            column)))))
         ;;Options was dataset-name so have to keep that pathway going.
         dataset-name (or (if (map? options)
                            (:dataset-name options)
                            options)
                          "_unnamed")
         column-seq (if (and (map? options)
                             (:key-fn options))
                      (let [key-fn (:key-fn options)]
                        (->> column-seq
                             (map #(ds-col/set-name
                                    %
                                    (key-fn (ds-col/column-name %))))))
                      column-seq)]
     (Dataset. (vec column-seq)
               (->> column-seq
                    (map-indexed
                     (fn [idx col]
                       [(ds-col/column-name col) idx]))
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
