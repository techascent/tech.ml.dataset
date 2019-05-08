(ns tech.libs.tablesaw
  (:require [tech.libs.tablesaw.tablesaw-column :as dtype-tbl]
            [tech.ml.dataset :as ds]
            [tech.ml.protocols.column :as col-proto]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.base :as dtype-base]
            [tech.v2.datatype.protocols :as dtype-proto]
            [clojure.set :as c-set]
            [tech.ml.dataset.seq-of-maps :as ds-seq-of-maps]
            [tech.ml.dataset.generic-columnar-dataset :as columnar-dataset]
            [tech.jna :as jna])
  (:import [tech.tablesaw.api Table ColumnType
            NumericColumn DoubleColumn
            StringColumn BooleanColumn]
           [tech.tablesaw.columns Column]
           [tech.tablesaw.io.csv CsvReadOptions]
           [java.util UUID]
           [org.apache.commons.math3.stat.descriptive.moment Skewness]))



(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defn- create-table-from-column-seq
  [^Table table col-seq]
  (Table/create (.name table) (into-array Column col-seq)))


(defn- column->metadata
  [^Column col]
  (merge {:name (.name col)}
         (when (= :string (dtype/get-datatype col))
           {:categorical? true})))


(declare make-column)


(defrecord TablesawColumn [^Column col metadata cache]
  col-proto/PIsColumn
  (is-column? [this] true)

  col-proto/PColumn
  (column-name [this] (or (:name metadata) (.name col)))
  (set-name [this colname]
    (->TablesawColumn col (assoc metadata :name colname) {}))

  (supported-stats [this] (col-proto/supported-stats col))

  (metadata [this] (merge metadata
                          {:name (col-proto/column-name this)
                           :size (dtype/ecount col)
                           :datatype (dtype/get-datatype col)}))

  (set-metadata [this data-map]
    (->TablesawColumn col data-map cache))

  (cache [this] cache)

  (set-cache [this cache-map]
    (->TablesawColumn col metadata cache-map))

  (missing [this] (col-proto/missing col))

  (unique [this] (col-proto/unique col))

  (stats [this stats-set]
    (when-not (instance? NumericColumn col)
      (throw (ex-info "Stats aren't available on non-numeric columns"
                      {:column-type (dtype/get-datatype col)
                       :column-name (col-proto/column-name this)
                       :column-java-type (type col)})))
    (let [stats-set (set (if-not (seq stats-set)
                           dtype-tbl/available-stats
                           stats-set))
          existing (->> stats-set
                        (map (fn [skey]
                               (when-let [cached (get metadata skey)]
                                 [skey cached])))
                        (remove nil?)
                        (into {}))
          missing-stats (c-set/difference stats-set (set (keys existing)))]
      (merge existing
             (col-proto/stats col missing-stats))))

  (correlation [this other-column correlation-type]
    (col-proto/correlation col (:col other-column) correlation-type))

  (column-values [this] (col-proto/column-values col))

  (is-missing? [this idx] (col-proto/is-missing? col idx))

  (select [this idx-seq] (make-column (col-proto/select col idx-seq) metadata {}))

  (empty-column [this datatype elem-count metadata]
    (dtype-proto/make-container :tablesaw-column datatype elem-count
                                (assoc (select-keys metadata [:name])
                                       :empty? true)))

  (new-column [this datatype elem-count-or-values metadata]
    (dtype-proto/make-container :tablesaw-column datatype
                                elem-count-or-values metadata))

  (clone [this]
    (dtype-proto/make-container :tablesaw-column
                                (dtype/get-datatype this)
                                (col-proto/column-values this)
                                metadata))

  (to-double-array [this error-missing?] (col-proto/to-double-array col error-missing?))

  dtype-proto/PDatatype
  (get-datatype [this] (dtype-base/get-datatype col))


  dtype-proto/PCopyRawData
  (copy-raw->item! [raw-data ary-target target-offset options]
    (dtype-proto/copy-raw->item! col ary-target target-offset options))

  dtype-proto/PPrototype
  (from-prototype [src datatype shape]
    (col-proto/new-column src datatype (first shape) (select-keys [:name] metadata)))


  dtype-proto/PToNioBuffer
  (convertible-to-nio-buffer? [item]
    (dtype-proto/convertible-to-nio-buffer? col))
  (->buffer-backing-store [item]
    (dtype-proto/as-nio-buffer col))


  dtype-proto/PToList
  (convertible-to-fastutil-list? [item]
    (dtype-proto/convertible-to-fastutil-list? col))
  (->list-backing-store [item]
    (dtype-proto/as-list col))


  dtype-proto/PToReader
  (convertible-to-reader? [item] true)
  (->reader [item options]
    (dtype-proto/->reader col options))


  dtype-proto/PToWriter
  (convertible-to-writer? [item] true)
  (->writer [item options]
    (dtype-proto/->writer col options))


  dtype-proto/PToIterable
  (convertible-to-iterable? [item] true)
  (->iterable [item options]
    (dtype-proto/->reader col options))


  dtype-proto/PToMutable
  (convertible-to-mutable? [item] true)
  (->mutable [item options]
    (dtype-proto/->mutable col options))

  dtype-proto/PBuffer
  (sub-buffer [item offset length]
    (->TablesawColumn
     (dtype-proto/sub-buffer col offset length)
     metadata {}))

  dtype-proto/PToArray
  (->sub-array [src] (dtype-proto/->sub-array col))
  (->array-copy [src] (dtype-proto/->array-copy col))

  dtype-proto/PCountable
  (ecount [item] (dtype-proto/ecount col)))


(defn make-column
  [datatype-col metadata & [cache]]
  (if (instance? TablesawColumn datatype-col)
    (throw (ex-info "Nested" {})))
  (->TablesawColumn datatype-col metadata cache))


(defmethod dtype-proto/make-container :tablesaw-column
  [container-type datatype elem-count-or-seq
   {:keys [empty?] :as options}]
  (when (and empty?
             (not (number? elem-count-or-seq)))
    (throw (ex-info "Empty columns must have colsize argument." {})))
  (->
   (if empty?
     (dtype-tbl/make-empty-column datatype elem-count-or-seq options)
     (dtype-tbl/make-column datatype elem-count-or-seq options))
   (make-column options {})))


(defn ^tech.tablesaw.io.csv.CsvReadOptions$Builder
  ->csv-builder [^String path & {:keys [separator header? date-format]}]
  (if separator
    (doto (CsvReadOptions/builder path)
      (.separator separator)
      (.header (boolean header?)))
    (doto (CsvReadOptions/builder path)
      (.header (boolean header?)))))


(defn tablesaw-columns->tablesaw-dataset
  [table-name columns]
  (columnar-dataset/make-dataset
   table-name
   (if (or (sequential? columns)
           (instance? java.util.List columns))
     (->> columns
          (mapv #(make-column % (column->metadata %))))
     (->> columns
          (mapv (fn [[col-name col]]
                  (make-column col (assoc (column->metadata col)
                                          :name col-name))))))
   {}))


(defn ->tablesaw-dataset
  [^Table table]
  (tablesaw-columns->tablesaw-dataset (.name table) (.columns table)))


(defn path->tablesaw-dataset
  [path & {:keys [separator quote]}]
  (-> (Table/read)
      (.csv (->csv-builder path :separator separator :header? true))
      ->tablesaw-dataset))


(defn col-dtype-cast
  [data-val dtype]
  (if (= dtype
         :string)
    (if (or (keyword? data-val)
            (symbol? data-val))
      (name data-val)
      (str data-val))
    (dtype/cast data-val dtype)))


(defn map-seq->tablesaw-dataset
  [map-seq-dataset {:keys [scan-depth
                           column-definitions
                           table-name]
                    :or {scan-depth 100
                         table-name "_unnamed"}
                    :as options}   ]

  (let [column-definitions
        (if column-definitions
          column-definitions
          (ds-seq-of-maps/autoscan-map-seq map-seq-dataset options))
        ;;force the dataset here as knowing the count helps
        column-map (->> column-definitions
                        (map (fn [{colname :name
                                   datatype :datatype
                                   :as coldef}]
                               (let [datatype (if (= datatype :keyword)
                                                :string
                                                datatype)]
                                 [colname
                                  (dtype-tbl/make-empty-column
                                   datatype 0 {:name colname})])))
                        (into {}))
        all-column-names (set (keys column-map))
        max-idx (reduce (fn [max-idx [idx item-row]]
                          (doseq [[item-name item-val] item-row]
                            (let [^Column col (get column-map item-name)
                                  missing (- (int idx) (.size col))]
                              (dotimes [idx missing]
                                (.appendMissing col))
                              (if-not (nil? item-val)
                                (.append col (col-dtype-cast
                                              item-val (dtype/get-datatype col)))
                                (.appendMissing col))))
                          idx)
                        0
                        (map-indexed vector map-seq-dataset))
        column-seq (vals column-map)
        max-ecount (long (if (seq column-seq)
                           (apply max (map dtype/ecount column-seq))
                           0))]
    ;;Ensure all columns are same length
    (doseq [^Column col column-seq]
      (let [missing-count (- max-ecount (.size col))]
        (dotimes [idx missing-count]
          (.appendMissing col))))
    (tablesaw-columns->tablesaw-dataset table-name column-map)))
