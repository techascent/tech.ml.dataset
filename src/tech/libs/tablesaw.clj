(ns tech.libs.tablesaw
  (:require [tech.libs.tablesaw.datatype.tablesaw :as dtype-tbl]
            [tech.ml.dataset :as ds]
            [tech.ml.protocols.column :as col-proto]
            [clojure.core.matrix.protocols :as mp]
            [tech.datatype :as dtype]
            [tech.datatype.base :as dtype-base]
            [tech.datatype.java-primitive :as primitive]
            [tech.parallel :as parallel]
            [clojure.set :as c-set]
            [tech.compute.tensor :as ct]
            [tech.compute.cpu.tensor-math :as cpu-tm]
            [tech.compute.cpu.typed-buffer :as cpu-typed-buffer]
            [tech.ml.dataset.compute-math-context :as compute-math-context]
            [tech.ml.dataset.seq-of-maps :as ds-seq-of-maps]
            [tech.ml.dataset.generic-columnar-dataset :as columnar-dataset]
            [tech.jna :as jna])
  (:import [tech.tablesaw.api Table ColumnType
            NumericColumn DoubleColumn
            StringColumn BooleanColumn]
           [tech.tablesaw.columns Column]
           [tech.tablesaw.io.csv CsvReadOptions]
           [java.util UUID]
           [org.apache.commons.math3.stat.descriptive.moment Skewness])
  (:import [tech.compute.cpu UnaryOp BinaryOp]))



(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defn- create-table-from-column-seq
  [^Table table col-seq]
  (Table/create (.name table) (into-array Column col-seq)))


(defn- col-datatype-cast
  [data-val col-dtype]
  (if-let [dtype ((set primitive/datatypes) col-dtype)]
    (dtype/cast data-val dtype)
    (case col-dtype
      :string (if (or (keyword? data-val)
                      (symbol? data-val))
                (name data-val)
                (str data-val))
      :boolean (boolean data-val))))


(defn- column->metadata
  [^Column col]
  (merge {:name (.name col)}
         (when (= :string (dtype/get-datatype col))
           {:categorical? true})))


(def available-stats
  (set [:mean
        :variance
        :median
        :min
        :max
        :skew
        :kurtosis
        :geometric-mean
        :sum-of-squares
        :sum-of-logs
        :quadratic-mean
        :standard-deviation
        :population-variance
        :sum
        :product
        :quartile-1
        :quartile-3]))


(declare make-column)


(defrecord TablesawColumn [^Column col metadata cache]
  col-proto/PIsColumn
  (is-column? [this] true)

  col-proto/PColumn
  (column-name [this] (or (:name metadata) (.name col)))
  (set-name [this colname]
    (->TablesawColumn col (assoc metadata :name colname) {}))

  (supported-stats [col] available-stats)

  (metadata [this] (assoc metadata
                          :name (col-proto/column-name this)
                          :size (mp/element-count col)
                          :datatype (dtype/get-datatype col)))

  (set-metadata [this data-map]
    (->TablesawColumn col data-map cache))

  (cache [this] cache)

  (set-cache [this cache-map]
    (->TablesawColumn col metadata cache-map))

  (missing [this]
    (-> (.isMissing ^Column col)
        (.toArray)))

  (unique [this]
    (-> (.unique ^Column col)
        (.asList)
        set))

  (stats [this stats-set]
    (when-not (instance? NumericColumn col)
      (throw (ex-info "Stats aren't available on non-numeric columns" {})))
    (let [stats-set (set (if-not (seq stats-set)
                           available-stats
                           stats-set))
          existing (->> stats-set
                        (map (fn [skey]
                               (when-let [cached (get metadata skey)]
                                 [skey cached])))
                        (remove nil?)
                        (into {}))
          missing-stats (c-set/difference stats-set (set (keys existing)))
          ^NumericColumn col col]
      (merge existing
             (->> missing-stats
                  (map (fn [skey]
                         [skey
                          (case skey
                            :mean (.mean col)
                            :variance (.variance col)
                            :median (.median col)
                            :min (.min col)
                            :max (.max col)
                            :skew (.skewness col)
                            :kurtosis (.kurtosis col)
                            :geometric-mean (.geometricMean col)
                            :sum-of-squares (.sumOfSquares col)
                            :sum-of-logs (.sumOfLogs col)
                            :quadratic-mean (.quadraticMean col)
                            :standard-deviation (.standardDeviation col)
                            :population-variance (.populationVariance col)
                            :sum (.sum col)
                            :product (.product col)
                            :quartile-1 (.quartile1 col)
                            :quartile-3 (.quartile3 col)
                            )]))
                  (into {})))))

  (correlation
    [this other-column correlation-type]
    (let [^NumericColumn column (jna/ensure-type NumericColumn col)
          ^NumericColumn other-column (jna/ensure-type NumericColumn (:col other-column))]
      (case correlation-type
        :pearson (.pearsons column other-column)
        :spearman (.spearmans column other-column)
        :kendall (.kendalls column other-column))))

  (column-values [this]
    (when-not (= 0 (dtype/ecount this))
      (or (dtype/->array this)
          (dtype/->array-copy this))))

  (is-missing? [this idx]
    (-> (.isMissing col)
        (.contains (int idx))))

  (get-column-value [this idx]
    (let [idx (int idx)]
      (when (< idx 0)
        (throw (ex-info "Index out of range" {:index idx})))
      (when (>= idx (.size col))
        (throw (ex-info "Index out of range" {:index idx})))
      (if-not (col-proto/is-missing? this idx)
        (.get col (int idx))
        (throw (ex-info (format "Column is missing index %s" idx) {})))))

  (set-values [this idx-val-seq]
    (let [new-col (.copy col)
          col-dtype (dtype/get-datatype col)]
      (doseq [[idx col-val] idx-val-seq]
        (.set new-col (int idx) (col-datatype-cast col-val col-dtype)))

      (make-column new-col metadata {})))

  (select [this idx-seq]
    (let [^ints int-data (if (instance? (Class/forName "[I") idx-seq)
                           idx-seq
                           (int-array idx-seq))]
      ;;We can't cache much metadata now as we don't really know.
      (make-column (.subset col int-data) metadata {})))

  (empty-column [this datatype elem-count metadata]
    (make-column
     (dtype-tbl/make-empty-column datatype elem-count
                                  {:column-name  (or (:name metadata)
                                                     (col-proto/column-name this))})
     metadata
     {}))

  (new-column [this datatype elem-count-or-values metadata]
    (make-column
     (dtype-tbl/make-column datatype elem-count-or-values
                            {:column-name (or (:name metadata)
                                              (col-proto/column-name this))})
     metadata
     {}))

  (clone [this]
    (make-column
     (dtype-tbl/make-column (dtype/get-datatype this) (col-proto/column-values this)
                            {:column-name (col-proto/column-name this)})
     metadata
     cache))

  (math-context [this]
    (compute-math-context/->ComputeTensorMathContext))

  dtype-base/PDatatype
  (get-datatype [this] (dtype-base/get-datatype col))

  dtype-base/PContainerType
  (container-type [this] (dtype-base/container-type col))

  dtype-base/PAccess
  (get-value [this idx] (dtype-base/get-value col idx))
  (set-value! [this offset val] (dtype-base/set-value! col offset val))
  (set-constant! [this offset value elem-count]
    (dtype-base/set-constant! col offset value elem-count))

  dtype-base/PCopyRawData
  (copy-raw->item! [raw-data ary-target target-offset options]
    (dtype-base/copy-raw->item! raw-data col target-offset options))

  dtype-base/PPrototype
  (from-prototype [src datatype shape]
    (col-proto/new-column src datatype (first shape) (select-keys [:name] metadata)))

  primitive/PToBuffer
  (->buffer-backing-store [item]
    (primitive/->buffer-backing-store col))

  primitive/POffsetable
  (offset-item [src offset]
    (primitive/offset-item col offset))

  primitive/PToArray
  (->array [src] (primitive/->array col))
  (->array-copy [src] (primitive/->array-copy col))

  mp/PElementCount
  (element-count [item] (mp/element-count col)))


;;Enable this to be used directly as a tensor.  This adds protocols telling the compute
;;system what device and driver this buffer pertains to.
(cpu-typed-buffer/generic-extend-java-type TablesawColumn)


(defn make-column
  [datatype-col metadata & [cache]]
  (->TablesawColumn datatype-col metadata cache))



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
                                          :name col-name))))))))


(defn ->tablesaw-dataset
  [^Table table]
  (tablesaw-columns->tablesaw-dataset (.name table) (.columns table)))


(defn path->tablesaw-dataset
  [path & {:keys [separator quote]}]
  (-> (Table/read)
      (.csv (->csv-builder path :separator separator :header? true))
      ->tablesaw-dataset))


(defn map-seq->tablesaw-dataset
  [map-seq-dataset {:keys [scan-depth
                           column-definitions
                           table-name]
                    :or {scan-depth 100
                         table-name "_unnamed"}
                    :as options}]

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
                                  (dtype-tbl/make-empty-column datatype 0 {:column-name colname})])))
                        (into {}))
        all-column-names (set (keys column-map))
        max-idx (reduce (fn [max-idx [idx item-row]]
                          (doseq [[item-name item-val] item-row]
                            (let [^Column col (get column-map item-name)
                                  missing (- (int idx) (.size col))]
                              (dotimes [idx missing]
                                (.appendMissing col))
                              (if-not (nil? item-val)
                                (.append col (col-datatype-cast item-val (dtype/get-datatype col)))
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
