(ns tech.v3.libs.arrow.copying
  (:require [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.datetime :as dtype-dt]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.packing :as packing]
            [tech.v3.dataset.base :as ds-base]
            [tech.v3.protocols.column :as col-proto]
            [tech.v3.dataset.impl.dataset :as ds-impl]
            [tech.v3.dataset.impl.column :as col-impl]
            [tech.v3.dataset.string-table :as str-table]
            [tech.v3.dataset.dynamic-int-list :as dyn-int-list]
            [tech.v3.dataset.utils :as ml-utils]
            [tech.v3.libs.arrow.schema :as arrow-schema]
            [tech.v3.libs.arrow.datatype :as arrow-dtype]
            [tech.v3.libs.arrow.allocator :as arrow-alloc]
            [clojure.edn :as edn]
            [tech.v3.io :as io])
  (:import
   ;;Behold -- My Kindom Of Nouns!!!
   [org.apache.arrow.vector.dictionary DictionaryProvider Dictionary
    DictionaryProvider$MapDictionaryProvider]
   [org.apache.arrow.vector.types.pojo Field Schema ArrowType$Int
    ArrowType$Utf8 ArrowType$Timestamp DictionaryEncoding]
   [org.apache.arrow.vector VarCharVector BaseFixedWidthVector
    BaseVariableWidthVector BaseLargeVariableWidthVector FieldVector
    DateDayVector VectorSchemaRoot  TimeStampMicroTZVector TimeStampMicroVector
    TimeStampMilliVector TimeStampMilliTZVector TimeStampSecVector
    TimeStampSecTZVector TimeStampNanoVector TimeStampNanoTZVector
    TimeSecVector TimeMilliVector TimeMicroVector TimeNanoVector]
   [org.apache.arrow.vector.ipc ArrowStreamReader ArrowStreamWriter]
   [tech.v3.dataset.string_table StringTable]
   [java.util HashMap]
   [java.time ZoneId]
   [java.io InputStream]))


(set! *warn-on-reflection* true)


(defn string-column->dict
  "Given a string column, return a map of {:dictionary :indices} which
  will be encoded according to the data in string-col->dict-id-table-width"
  ^Dictionary [col]
  (let [str-t (ds-base/ensure-column-string-table col)
        indices (str-table/indices str-t)
        int->str (str-table/int->string str-t)
        bit-width (casting/int-width (dtype/elemwise-datatype indices))
        metadata (meta col)
        colname (:name metadata)
        dict-id (.hashCode ^Object colname)
        arrow-indices-type (ArrowType$Int. bit-width true)
        encoding (DictionaryEncoding. dict-id false arrow-indices-type)
        varchar-vec (arrow-dtype/strings->varchar!
                     (dtype/->reader int->str)
                     nil
                     (VarCharVector. "unnamed" (arrow-alloc/allocator)))]
    (Dictionary. varchar-vec encoding)))


(defn string-col->encoding
  "Given a string column return a map of :dict-id :table-width.  The dictionary
  id is the hashcode of the column mame."
  [^DictionaryProvider$MapDictionaryProvider dict-provider _colname col]
  (let [dict (string-column->dict col)]
    (.put dict-provider ^Dictionary dict)
    {:encoding (.getEncoding dict)}))


(defn idx-col->field
  ^Field [dict-provider {:keys [strings-as-text?]} ^long _idx col]
  (let [colmeta (meta col)
        nullable? (boolean
                   (or (:nullable? colmeta)
                       (not (.isEmpty
                             (col-proto/missing col)))))
        col-dtype (:datatype colmeta)
        colname (:name colmeta)
        extra-data (merge (select-keys (meta col) [:timezone])
                          (when (and (not strings-as-text?)
                                     (= :string col-dtype))
                            (string-col->encoding dict-provider colname col)))]
    (try
      (arrow-schema/make-field
       (ml-utils/column-safe-name colname)
       (arrow-schema/datatype->field-type col-dtype nullable? colmeta extra-data))
      (catch Throwable e
        (throw (Exception. (format "Column %s metadata conversion failure:\n%s"
                                   colname e)
                           e))))))


(defn ds->arrow-schema
  ([ds options]
   (let [dict-provider (DictionaryProvider$MapDictionaryProvider.
                        (make-array Dictionary 0))]
     {:schema
      (Schema. ^Iterable
               (->> (ds-base/columns ds)
                    (map-indexed (partial idx-col->field dict-provider options))))
      :dict-provider dict-provider}))
  ([ds]
   (ds->arrow-schema ds {})))


(defn copy-column->arrow!
  ^FieldVector [col missing ^FieldVector field-vec]
  (let [dtype (dtype/elemwise-datatype col)
        ft (-> (.getField field-vec)
               (.getType))]
    (if (or (= dtype :text)
            (instance? ArrowType$Utf8 ft))
      (arrow-dtype/strings->varchar! col missing field-vec)
      (do
        (when-not (instance? BaseFixedWidthVector field-vec)
          (throw (Exception. (format "Input is not a fixed-width vector"))))
        (let [n-elems (dtype/ecount col)
              ^BaseFixedWidthVector field-vec field-vec
              _ (do (.allocateNew field-vec n-elems)
                    (.setValueCount field-vec n-elems))
              valid-buf (.getValidityBuffer field-vec)
              data (if (= :string dtype)
                     (-> (ds-base/column->string-table col)
                         (str-table/indices))
                     col)]
          (arrow-dtype/missing->valid-buf missing valid-buf n-elems)
          (dtype/copy! data field-vec))))
    field-vec))


(defn ->timezone
  (^ZoneId [& [item]]
   (cond
     (instance? ZoneId item)
     item
     (string? item)
     (ZoneId/of ^String item)
     :else
     (dtype-dt/utc-zone-id))))


(defn datetime-cols->epoch
  [ds {:keys [timezone]}]
  (let [timezone (when timezone (->timezone timezone))]
    (reduce
     (fn [ds col]
       (let [col-dt (packing/unpack-datatype (dtype/elemwise-datatype col))]
         (if (and (not= col-dt :local-time)
                  (dtype-dt/datetime-datatype? col-dt))
           (assoc ds
                  (col-proto/column-name col)
                  (col-impl/new-column
                   (col-proto/column-name col)
                   (dtype-dt/datetime->epoch
                    timezone
                    (if (= :local-date (packing/unpack-datatype col-dt))
                      :epoch-days
                      :epoch-milliseconds)
                    col)
                   (assoc (meta col)
                          :timezone (str timezone)
                          :source-datatype (dtype/elemwise-datatype col))
                   (col-proto/missing col)))
           ds)))
     ds
     (ds-base/columns ds))))


(defn copy-ds->vec-root
  [^VectorSchemaRoot vec-root ds]
  (.setRowCount vec-root (ds-base/row-count ds))
  (->> (ds-base/columns ds)
       (map-indexed
        (fn [^long idx col]
          (let [field-vec (.getVector vec-root idx)
                vec-type (.getType (.getField field-vec))
                ;;Ensure packed as this allows localtime to convert to
                ;;packed local time and datetime types have already been dealt with
                coldata (packing/pack col)
                col-type (dtype/elemwise-datatype coldata)
                missing (col-proto/missing col)]
            (cond
              (and (= :string col-type)
                   (not (instance? ArrowType$Utf8 vec-type)))
              (-> (ds-base/column->string-table col)
                  (str-table/indices)
                  (copy-column->arrow! missing field-vec))
              :else
              (copy-column->arrow! coldata missing field-vec)))))
       (dorun)))


(defn- prepare-dataset-for-write
  [ds options]
  (cond-> (datetime-cols->epoch ds options)
    (not (:strings-as-text? options))
    (ds-base/ensure-dataset-string-tables)))


(defn write-dataset-to-stream!
  "Write a dataset as an arrow stream file.  File will contain one record set.

  Options:

  * `strings-as-text?`: - defaults to false - Save out strings into arrow files without
     dictionaries.  This works well if you want to load an arrow file in-place or if
     you know the strings in your dataset are either really large or should not be in
     string tables."
  ([ds path options]
   (let [ds (prepare-dataset-for-write ds options)
         {:keys [schema dict-provider]} (ds->arrow-schema ds options)
         ^DictionaryProvider dict-provider dict-provider]
     (with-open [ostream (io/output-stream! path)
                 vec-root (VectorSchemaRoot/create
                           ^Schema schema
                           ^BufferAllocator (arrow-alloc/allocator))
                 writer (ArrowStreamWriter. vec-root dict-provider ostream)]
       (.start writer)
       (copy-ds->vec-root vec-root ds)
       (.writeBatch writer)
       (.end writer))))
  ([ds path]
   (write-dataset-to-stream! ds path {})))


(defn write-dataset-seq-to-stream!
  "Write a sequence of datasets to a stream.  Datasets are written with doseq.
  All datasets must be amenable to being written into vectors of the type dictated
  by the schema of the first dataset.  Each dataset is written to a separate batch.


  * `strings-as-text?`: - defaults to false - Save out strings into arrow files without
     dictionaries.  This works well if you want to load an arrow file in-place or if
     you know the strings in your dataset are either really large or should not be in
     string tables."
  ([ds-seq path options]
   (let [ds-seq (map #(prepare-dataset-for-write % options) ds-seq)
         ds (first ds-seq)
         {:keys [schema dict-provider]} (ds->arrow-schema ds options)
         ^DictionaryProvider dict-provider dict-provider]
     (with-open [ostream (io/output-stream! path)
                 vec-root (VectorSchemaRoot/create
                           ^Schema schema
                           ^BufferAllocator (arrow-alloc/allocator))
                 writer (ArrowStreamWriter. vec-root dict-provider ostream)]
       (.start writer)
       (doseq [ds ds-seq]
         (copy-ds->vec-root vec-root ds)
         (.writeBatch writer))
       (.end writer))))
  ([ds path]
   (write-dataset-seq-to-stream! ds path {})))


#_(defn- write-dataset-to-file!
  "EXPERIMENTAL & NOT WORKING - please use streaming formats for now."
  ([ds path options]
   (let [ds (ds-base/ensure-dataset-string-tables ds)
         ds (datetime-cols-to-millis-from-epoch ds options)
         {:keys [schema dict-provider]} (ds->arrow-schema ds)
         ^DictionaryProvider dict-provider dict-provider]
     (with-open [ostream (java.io.RandomAccessFile. ^String path "rw")
                 vec-root (VectorSchemaRoot/create
                           ^Schema schema
                           ^BufferAllocator (arrow-alloc/allocator))
                 writer (ArrowFileWriter. vec-root dict-provider
                                          (.getChannel ostream))]
       (.start writer)
       (copy-ds->vec-root vec-root ds)
       (.writeBatch writer)
       (.end writer))))
  ([ds path]
   (write-dataset-to-file! ds path {})))


(defprotocol PFieldVecMeta
  (field-vec-metadata [fv]))


(defn- timezone-from-field-vec
  [^FieldVector fv]
  (let [ft (-> (.getField fv)
               (.getType))]
    (when (instance? ArrowType$Timestamp ft)
      (.getTimezone ^ArrowType$Timestamp ft))))


(extend-protocol PFieldVecMeta
  Object
  (field-vec-metadata [fv] {})
  DateDayVector
  (field-vec-metadata [fv] {:time-unit :epoch-days})
  TimeSecVector (field-vec-metadata [fv] {:time-unit :time-seconds})
  TimeMilliVector (field-vec-metadata [fv] {:time-unit :time-milliseconds})
  TimeMicroVector (field-vec-metadata [fv] {:time-unit :time-microseconds})
  TimeNanoVector (field-vec-metadata [fv] {:time-unit :time-nanoseconds})
  TimeStampNanoVector
  (field-vec-metadata [fv] {:time-unit :epoch-nanosecond})
  TimeStampNanoTZVector
  (field-vec-metadata [fv] {:time-unit :epoch-nanosecond
                            :timezone (timezone-from-field-vec fv)})
  TimeStampMicroVector
  (field-vec-metadata [fv] {:time-unit :epoch-microsecond})
  TimeStampMicroTZVector
  (field-vec-metadata [fv] {:time-unit :epoch-microsecond
                            :timezone (timezone-from-field-vec fv)})
  TimeStampMilliVector
  (field-vec-metadata [fv] {:time-unit :epoch-millisecond})
  TimeStampMilliTZVector
  (field-vec-metadata [fv] {:time-unit :epoch-millisecond
                            :timezone (timezone-from-field-vec fv)})

  TimeStampSecVector
  (field-vec-metadata [fv] {:time-unit :epoch-second})
  TimeStampSecTZVector
  (field-vec-metadata [fv] {:time-unit :epoch-second
                            :timezone (timezone-from-field-vec fv)}))


(defn field-vec->column
  [{:keys [epoch->datetime?]}
   dict-map
   [^long idx ^FieldVector fv]]
  (let [field (.getField fv)
        n-elems (dtype/ecount fv)
        colname (if (and (.getName fv)
                         (not= (count (.getName fv)) 0))
                  (.getName fv)
                  (str "column-" idx))
        ft (.getFieldType field)
        encoding (.getDictionary ft)
        ^Dictionary dict (when encoding
                           (get dict-map (.getId encoding)))
        metadata (try (->> (.getMetadata field)
                           (map (fn [[k v]]
                                  (try
                                    [(edn/read-string k)
                                     (edn/read-string v)]
                                    (catch Exception _e
                                      [k v]))))
                           (into {}))
                      (catch Throwable e
                        (throw
                         (Exception.
                          (format "Failed to deserialize metadata: %s\n%s"
                                  e
                                  (.getMetadata field))))))
        valid-buf (.getValidityBuffer fv)
        missing (arrow-dtype/valid-buf->missing valid-buf n-elems)
        ;;Aside from actual metadata saved with the field vector, some field vector
        ;;types generate their own bit of metadata
        metadata (merge metadata (field-vec-metadata fv))
        fv-dtype (dtype/elemwise-datatype fv)
        _ (println "fv-dtype" fv-dtype (type fv))
        coldata
        (cond
          dict
          (let [strs (arrow-dtype/dictionary->strings dict)
                data (dyn-int-list/make-from-container (dtype/->array-buffer fv))
                n-table-elems (dtype/ecount strs)
                str->int (HashMap. n-table-elems)]
            (dotimes [idx n-table-elems]
              (.put str->int (.get strs idx) idx))
            (StringTable. strs str->int data))
          (and (instance? BaseVariableWidthVector fv)
               (.getOffsetBuffer fv))
          (-> (dtype/->buffer (arrow-dtype/varchar->strings fv))
              (arrow-dtype/string-reader->text-reader))
          (and (instance? BaseLargeVariableWidthVector fv)
               (.getOffsetBuffer fv))
          (-> (dtype/->buffer (arrow-dtype/large-varchar->strings fv))
              (arrow-dtype/string-reader->text-reader))
          ;;Mapping back to local-dates takes a bit of time.  This is only
          ;;necessary if you really need them.
          (and epoch->datetime?
               (arrow-dtype/epoch-datatypes fv-dtype))
          (->> (dtype/clone fv)
               (dtype-dt/epoch->datetime
                (:timezone metadata)
                (or (:source-datatype metadata)
                    (arrow-dtype/default-datetime-datatype fv-dtype))))
          :else
          (dtype/->array-buffer fv))
        new-col
        (col-impl/new-column (or (:name metadata) colname)
                             coldata metadata missing)]
    new-col))


(defn arrow->ds
  [ds-name ^VectorSchemaRoot schema-root dict-map options]
  (->> (.getFieldVectors schema-root)
       (map-indexed vector)
       (mapv (partial field-vec->column options dict-map))
       (ds-impl/new-dataset ds-name)))


(defn do-load-dataset-seq
  [^InputStream istream ^ArrowStreamReader reader path idx options]
  (if (.loadNextBatch reader)
    (cons (arrow->ds (format "%s-%03d" path idx)
                     (.getVectorSchemaRoot reader)
                     (.getDictionaryVectors reader)
                     options)
          (lazy-seq (do-load-dataset-seq istream reader path (inc idx) options)))
    (do (.close reader)
        (.close istream)
        nil)))


(defn stream->dataset-seq-copying
  "Read a complete arrow file lazily.  Each data record is copied into an
  independent dataset.  Stream is closed when the last dataset is loaded."
  ([path options]
   (let [istream (io/input-stream path)
         reader (ArrowStreamReader. istream (arrow-alloc/allocator))]
     (do-load-dataset-seq istream reader path 0 options)))
  ([path]
   (stream->dataset-seq-copying path {})))


(defn read-stream-dataset-copying
  "Read a single record batch and return a dataset.  It is an error if there
  are more record batches in the file."
  ([path options]
   (with-open [istream (io/input-stream path)
               reader (ArrowStreamReader. istream (arrow-alloc/allocator))]
     (when (.loadNextBatch reader)
       (let [retval
             (arrow->ds path
                        (.getVectorSchemaRoot reader)
                        (.getDictionaryVectors reader)
                        options)]
         (when (.loadNextBatch reader)
           (throw (Exception. "File contains multiple batches.
Please use `stream->dataset-seq-copying`")))
         retval))))
  ([path]
   (read-stream-dataset-copying path {})))


(comment
  (require '[tech.v3.dataset :as ds])
  (def stocks (ds/->dataset "test/data/stocks.csv"))
  (write-dataset-to-stream! stocks "test.arrow" {:timezone "US/Eastern"})
  (def big-stocks (apply ds/concat-copying (repeat 10000 stocks)))
  (write-dataset-to-stream! big-stocks "big-stocks.feather")
  (write-dataset-to-file! big-stocks "big-stocks.file.feather")
  (io/put-nippy! "big-stocks.nippy" big-stocks)
  )
