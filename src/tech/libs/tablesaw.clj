(ns tech.libs.tablesaw
  (:require [tech.libs.tablesaw.tablesaw-column :as dtype-tbl]
            [tech.ml.protocols.column :as col-proto]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.base :as dtype-base]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.pprint :as dtype-pp]
            [clojure.set :as c-set]
            [tech.ml.dataset.seq-of-maps :as ds-seq-of-maps]
            [tech.ml.dataset.generic-columnar-dataset :as columnar-dataset]
            [tech.io :as io]
            [clojure.data.csv :as csv])
  (:import [tech.tablesaw.api Table ColumnType
            NumericColumn DoubleColumn
            StringColumn BooleanColumn]
           [tech.tablesaw.columns Column]
           [tech.tablesaw.io.csv CsvReadOptions
            CsvReadOptions$Builder]
           [tech.tablesaw.io Source]
           [java.util UUID]
           [java.io InputStream BufferedInputStream
            ByteArrayInputStream]
           [org.apache.commons.math3.stat.descriptive.moment Skewness]
           [tech.tablesaw.io ColumnTypeDetector ReadOptions ReadOptions$Builder]
           [tech.v2.datatype ObjectReader]))



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


(deftype TablesawColumn [^Column col metadata cache]
  col-proto/PIsColumn
  (is-column? [this] true)

  col-proto/PColumn
  (column-name [this] (or (:name metadata) (.name col)))
  (set-name [this colname]
    (TablesawColumn. col (assoc metadata :name colname) {}))

  (supported-stats [this] (col-proto/supported-stats col))

  (metadata [this] (merge metadata
                          {:name (col-proto/column-name this)
                           :size (dtype/ecount col)
                           :datatype (dtype/get-datatype col)}))

  (set-metadata [this data-map]
    (TablesawColumn. col data-map cache))

  (cache [this] cache)

  (set-cache [this cache-map]
    (TablesawColumn. col metadata cache-map))

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
    (col-proto/correlation col
                           (.col ^TablesawColumn other-column)
                           correlation-type))

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

  (to-double-array [this error-missing?]
    (col-proto/to-double-array col error-missing?))

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
    (TablesawColumn.
     (dtype-proto/sub-buffer col offset length)
     metadata {}))

  dtype-proto/PToArray
  (->sub-array [src] (dtype-proto/->sub-array col))
  (->array-copy [src] (dtype-proto/->array-copy col))

  dtype-proto/PCountable
  (ecount [item] (dtype-proto/ecount col))

  ObjectReader
  (lsize [item] (long (dtype-proto/ecount col)))
  (read [item idx]
    (.get col idx))

  Object
  (toString [item]
    (let [n-items (dtype/ecount item)
          format-str (if (> n-items 20)
                       "#tablesaw-column<%s>%s\n%s\n[%s...]"
                       "#tablesaw-column<%s>%s\n%s\n[%s]")]
      (format format-str
              (name (dtype/get-datatype item))
              [n-items]
              (col-proto/column-name item)
              (-> (dtype/->reader item)
                  (dtype-proto/sub-buffer 0 (min 20 n-items))
                  (dtype-pp/print-reader-data))))))


(defmethod print-method TablesawColumn
  [col ^java.io.Writer w]
  (.write w (.toString ^Object col)))


(defn make-column
  [datatype-col metadata & [cache]]
  (if (instance? TablesawColumn datatype-col)
    (throw (ex-info "Nested" {})))
  (TablesawColumn. datatype-col metadata cache))


(defmethod dtype-proto/make-container :tablesaw-column
  [_container-type datatype elem-count-or-seq
   {:keys [empty?] :as options}]
  (when (and empty?
             (not (number? elem-count-or-seq)))
    (throw (ex-info "Empty columns must have colsize argument." {})))
  (->
   (if empty?
     (dtype-tbl/make-empty-column datatype elem-count-or-seq options)
     (dtype-tbl/make-column datatype elem-count-or-seq options))
   (make-column options {})))


(defn autodetect-csv-separator
  [^BufferedInputStream input-stream & options]
  (.mark input-stream 1000)
  (let [byte-data (byte-array 1000)
        num-read (.read input-stream byte-data)
        _ (.reset input-stream)]
    (apply io/autodetect-csv-separator (ByteArrayInputStream. byte-data 0 num-read)
           options)))


(defmulti keyword->tablesaw-column-type
  (fn [col-type-kwd]
    col-type-kwd))


(defmethod keyword->tablesaw-column-type :default
  [col-type-kwd]
  (if (instance? ColumnType col-type-kwd)
    col-type-kwd
    (case col-type-kwd
      :int16 ColumnType/SHORT
      :int32 ColumnType/INTEGER
      :int64 ColumnType/LONG
      :float32 ColumnType/FLOAT
      :float64 ColumnType/DOUBLE
      :boolean ColumnType/BOOLEAN
      :string ColumnType/STRING)))


(defn to-minimal-csv-seq
  "Read a smaller csv up to a given character limit."
  [^BufferedInputStream input-stream options]
  ;;Need enough bytes that we can reliably autodetect a lot of files.
  (let [num-bytes (long (or (:autodetect-max-bytes options)
                            65536))
        _ (.mark input-stream num-bytes)
        byte-data (byte-array num-bytes)
        bytes-read (.read input-stream byte-data)
        _ (.reset input-stream)
        ;;the last line must be incomplete
         csv-seq (-> (ByteArrayInputStream. byte-data 0 bytes-read)
                     (io/reader)
                     (csv/read-csv :separator (:separator options)))]
    ;;deal with last line exception
    (loop [cleaned-seq []
           csv-seq csv-seq]
      (let [next-line
            (try
              (first csv-seq)
              (catch Throwable _e
                nil))]
        (if-not next-line
          cleaned-seq
          (recur (conj cleaned-seq next-line)
                 (rest csv-seq)))))))


(defn autodetect-column-types
  [^BufferedInputStream input-stream column-type-detect-fn options]
  (->> (to-minimal-csv-seq input-stream options)
       (column-type-detect-fn)
       (map keyword->tablesaw-column-type)
       (into-array ColumnType)))


(defn extended-column-types
  []
  (let [rdr-type ReadOptions
        target-field (.getDeclaredField rdr-type "EXTENDED_TYPES")
        _ (.setAccessible target-field true)]
    (.get target-field nil)))


(defn construct-read-options
  ^ReadOptions []
  (let [ctor (.getDeclaredConstructor ReadOptions$Builder (make-array Class 0))
        _ (.setAccessible ctor true)
        builder (.newInstance ctor (make-array Object 0))
        build-method (.getDeclaredMethod ^Class (type builder)
                                         "build" (make-array Class 0))]
    (.setAccessible build-method true)
    (.invoke build-method builder (make-array Object 0))))


(defn tablesaw-detect-column-types
  [^BufferedInputStream input-stream options]
  (let [read-options (construct-read-options)
        detector (ColumnTypeDetector. ^Java.util.List (extended-column-types))
        ^java.lang.Iterable iterable
        (->> (to-minimal-csv-seq input-stream options)
             ;;drop column names
             (rest)
             (map #(into-array String %)))]
    (.detectColumnTypes detector
                        (.iterator iterable)
                        read-options)))


(defn ^CsvReadOptions$Builder
  ->csv-builder [path & options]
  (let [^BufferedInputStream input-stream (apply io/buffered-input-stream
                                                 path options)
        separator (apply autodetect-csv-separator input-stream options)
        opt-map (assoc (apply hash-map options) :separator separator)
        ;;We have to detect all of the column types here.
        column-types (if-let [coltypes (:column-types opt-map)]
                       (map keyword->tablesaw-column-type coltypes)
                       (if-let [col-fn (:column-type-fn opt-map)]
                         (autodetect-column-types input-stream col-fn opt-map)
                         (tablesaw-detect-column-types input-stream opt-map)))]
    (cond-> (CsvReadOptions/builder input-stream)
      true
      (.separator separator)
      true
      (.header (boolean (if (nil? (:header? opt-map))
                          true
                          (:header? opt-map))))
      column-types
      (.columnTypes
       (into-array ColumnType
                   ^"[Ltech.tablesaw.api.ColumnType;"
                   column-types)))))


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
  [path & options]
  (let [input (if (and (string? path)
                       (.endsWith ^String path ".gz"))
                (io/gzip-input-stream path)
                path)]
       (-> (Table/read)
           (.csv ^CsvReadOptions$Builder (apply ->csv-builder input options))
           ->tablesaw-dataset)))


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
  "Scan a sequence of maps and produce a tablesaw dataset."
  [map-seq-dataset {:keys [column-definitions
                           table-name]
                    :or {table-name "_unnamed"}
                    :as options}]
  (let [column-definitions
        (if column-definitions
          column-definitions
          (ds-seq-of-maps/autoscan-map-seq map-seq-dataset options))
        ;;force the dataset here as knowing the count helps
        column-map (->> column-definitions
                        (map (fn [{colname :name
                                   datatype :datatype}]
                               (let [datatype (if (= datatype :keyword)
                                                :string
                                                datatype)]
                                 [colname
                                  (dtype-tbl/make-empty-column
                                   datatype 0 {:name colname})])))
                        (into {}))
        _ (->> map-seq-dataset
               (map-indexed
                (fn [idx item-row]
                  (doseq [[item-name item-val] item-row]
                    (let [^Column col (get column-map item-name)
                          missing (- (int idx) (.size col))]
                      (dotimes [_idx missing]
                        (.appendMissing col))
                      (if-not (nil? item-val)
                        (.append col (col-dtype-cast
                                      item-val (dtype/get-datatype col)))
                        (.appendMissing col))))))
               (dorun))
        column-seq (vals column-map)
        max-ecount (long (if (seq column-seq)
                           (apply max (map dtype/ecount column-seq))
                           0))]
    ;;Ensure all columns are same length
    (doseq [^Column col column-seq]
      (let [missing-count (- max-ecount (.size col))]
        (dotimes [_idx missing-count]
          (.appendMissing col))))
    (tablesaw-columns->tablesaw-dataset table-name column-map)))
