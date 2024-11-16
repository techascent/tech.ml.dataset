(ns tech.v3.libs.clj-transit
  "Transit bindings for the jvm version of tech.v3.dataset."
  (:require [tech.v3.dataset :as ds]
            [tech.v3.dataset.impl.dataset :as ds-impl]
            [tech.v3.dataset.io :as ds-io]
            [tech.v3.dataset.dynamic-int-list :as int-list]
            [tech.v3.io :as io]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.packing :as packing]
            [tech.v3.datatype.datetime :as dtype-dt]
            [tech.v3.datatype.array-buffer :as abuf]
            [tech.v3.datatype.bitmap :as bitmap]
            [clj-commons.primitive-math :as pmath]
            [ham-fisted.api :as hamf]
            [cognitect.transit :as t])
  (:import [tech.v3.dataset.impl.dataset Dataset]
           [tech.v3.dataset.impl.column Column]
           [tech.v3.dataset Text]
           [java.nio ByteBuffer ByteOrder]
           [java.nio.charset StandardCharsets]
           [java.util Base64 HashMap]
           [java.time LocalDate Instant]))

(set! *warn-on-reflection* true)


(defn- le-bbuf
  ^ByteBuffer [n-elems]
  (-> (ByteBuffer/wrap (byte-array n-elems))
      (.order ByteOrder/LITTLE_ENDIAN)))

(defn- numeric-data->b64
  [col]
  (let [src-data (dtype/->reader col)
        n-elems (count col)
        ^ByteBuffer bbuf
        (case (casting/host-flatten (dtype/elemwise-datatype col))
          :int8
          (let [retval (le-bbuf n-elems)
                dst-data (dtype/->buffer retval)]
            (dotimes [idx (count col)]
              (.writeByte dst-data idx (unchecked-byte (.readLong src-data idx))))
            retval)
          :int16
          (let [retval (le-bbuf (* Short/BYTES n-elems))
                dst-data (.asShortBuffer retval)]
            (dotimes [idx (count col)]
              (.put dst-data idx (unchecked-short (.readLong src-data idx))))
            retval)
          :int32
          (let [retval (le-bbuf (* Integer/BYTES n-elems))
                dst-data (.asIntBuffer retval)]
            (dotimes [idx (count col)]
              (.put dst-data idx (unchecked-int (.readLong src-data idx))))
            retval)
          :int64
          (let [retval (le-bbuf (* Long/BYTES n-elems))
                dst-data (.asLongBuffer retval)]
            (dotimes [idx (count col)]
              (.put dst-data idx (.readLong src-data idx)))
            retval)
          :float32
          (let [retval (le-bbuf (* Float/BYTES n-elems))
                dst-data (.asFloatBuffer retval)]
            (dotimes [idx (count col)]
              (.put dst-data idx (.readDouble src-data idx)))
            retval)
          :float64
          (let [retval (le-bbuf (* Double/BYTES n-elems))
                dst-data (.asDoubleBuffer retval)]
            (dotimes [idx (count col)]
              (.put dst-data idx (.readDouble src-data idx)))
            retval))]
    (String. (.encode (Base64/getEncoder) (.array bbuf)))))


(defn- string-col->data
  [col]
  (let [strdata (dtype/make-list :string)
        indexes (dtype/make-list :int32)
        seen (HashMap.)]
    (dotimes [idx (count col)]
      (let [strval (str (or (col idx) ""))
            idx (.computeIfAbsent seen strval (reify java.util.function.Function
                                                (apply [this arg]
                                                  (let [retval (count strdata)]
                                                    (.add strdata strval)
                                                    retval))))]
        (.add indexes idx)))
    {:strtable (hamf/object-array strdata)
     :indexes (numeric-data->b64 indexes)}))


;;The assumption here is that transit has already optimized a vector or strings.
(defn- text-col->data
  [col]
  (let [offsets (int-list/dynamic-int-list 0)
        data (StringBuilder.)
        n-elems (dtype/ecount col)]
    (dotimes [idx n-elems]
      (let [next-data (str (or (col idx) ""))]
        (.append data next-data)
        (.addLong offsets (.length data))))
    (let [offbuf (dtype/as-concrete-buffer offsets)]
      {:offset-dtype (dtype/elemwise-datatype offbuf)
       :offsets (numeric-data->b64 offbuf)
       :buffer (.toString data)})))


(defn- obj-col->numeric-b64
  [col dst-dtype convert-fn]
  (let [data (dtype/make-list dst-dtype)]
    (dotimes [idx (count col)]
      (.add data (if-let [colval (col idx)]
                   (convert-fn colval)
                   0)))
    (numeric-data->b64 data)))


(defn- col->data
  [col]
  (let [col-dt (packing/unpack-datatype (dtype/elemwise-datatype col))]
    {:metadata (assoc (meta col) :datatype col-dt)
     :missing (vec (-> (ds/missing col)
                       (bitmap/->random-access)
                       (hamf/object-array)
                       vec))
     :data
     (cond
       (casting/numeric-type? col-dt)
       (numeric-data->b64 col)
       (= :boolean col-dt)
       (numeric-data->b64 (dtype/make-reader  :uint8 (count col)
                                             (if (col idx) 1 0)))
       (= :string col-dt)
       (string-col->data col)
       (= :text col-dt)
       (text-col->data col)
       (#{:packed-local-date :local-date} col-dt)
       (obj-col->numeric-b64 col :int32 dtype-dt/local-date->days-since-epoch)
       (#{:packed-instant :instant} col-dt)
       (obj-col->numeric-b64 col :int64 dtype-dt/instant->microseconds-since-epoch)
       :else ;;Punt!!
       (vec col))}))


(defn ^:no-doc dataset->data
  "Dataset to transit-safe data"
  [ds]
  {:metadata (meta ds)
   :flavor :transit
   :version 1
   :columns (mapv col->data (ds/columns ds))})


(defn ^:no-doc b64->numeric-data
  [^String data dtype]
  (let [byte-data (.decode (Base64/getDecoder) (.getBytes data))
        bbuf (-> (ByteBuffer/wrap byte-data)
                 (.order ByteOrder/LITTLE_ENDIAN))]
    (case dtype
      :int8 (dtype/->buffer byte-data)
      :uint8 (dtype/make-reader :uint8 (alength byte-data)
                                (pmath/byte->ubyte (aget byte-data idx)))
      :int16 (let [sbuf (.asShortBuffer bbuf)]
               (dtype/make-reader :int16 (.limit sbuf) (.get sbuf idx)))
      :uint16 (let [sbuf (.asShortBuffer bbuf)]
                (dtype/make-reader :uint16 (.limit sbuf) (pmath/short->ushort
                                                          (.get sbuf idx))))
      :int32 (let [ibuf (.asIntBuffer bbuf)]
               (dtype/make-reader :int32 (.limit ibuf) (.get ibuf idx)))
      :uint32 (let [ibuf (.asIntBuffer bbuf)]
                (dtype/make-reader :uint32 (.limit ibuf) (pmath/int->uint (.get ibuf idx))))
      :int64 (let [lbuf (.asLongBuffer bbuf)]
               (dtype/make-reader :int64 (.limit lbuf) (.get lbuf idx)))
      :uint64 (let [lbuf (.asLongBuffer bbuf)]
                (dtype/make-reader :uint64 (.limit lbuf) (.get lbuf idx)))
      :float32 (let [fbuf (.asFloatBuffer bbuf)]
                 (dtype/make-reader :float32 (.limit fbuf) (.get fbuf idx)))
      :float64 (let [fbuf (.asDoubleBuffer bbuf)]
                 (dtype/make-reader :float64 (.limit fbuf) (.get fbuf idx))))))


(defn ^:no-doc str-data->coldata
  [{:keys [strtable indexes]}]
  (let [indexes (dtype/->reader (b64->numeric-data indexes :int32))
        strdata (dtype/->reader strtable)]
    (dtype/make-reader :string (count indexes) (strdata (indexes idx)))))


(defn ^:no-doc text-data->coldata
  [{:keys [offsets buffer offset-dtype]}]
  (let [offsets (dtype/->buffer (b64->numeric-data offsets offset-dtype))
        n-elems (dtype/ecount offsets)
        buffer (str buffer)]
    (-> (dtype/make-reader
         :text n-elems
         (let [prev-idx (unchecked-dec idx)
               prev-offset (long (if (> prev-idx -1)
                                   (.readLong offsets prev-idx)
                                   0))]
           (Text. (.substring buffer prev-offset (.readLong offsets idx))))))))


(defn ^:no-doc data->dataset
  [{:keys [columns] :as ds-data}]
  (let [ds-meta (:metadata ds-data)]
    (errors/when-not-errorf
      (and ds-meta columns)
      "Passed in data does not appear to have metadata or columns")
    (->> (:columns ds-data)
         (map
           (fn [{:keys [metadata missing data]}]
             (let [dtype (:datatype metadata)]
               #:tech.v3.dataset{:metadata metadata
                                 :missing (bitmap/->bitmap missing)
                                 ;;do not re-scan data.
                                 :force-datatype? true
                                 :data
                                  (cond
                                    (casting/numeric-type? dtype)
                                    (b64->numeric-data data dtype)
                                    (= :boolean dtype)
                                    (let [ibuf (b64->numeric-data data :int8)]
                                      (dtype/make-reader :boolean (count ibuf)
                                                         (if (== 0 (unchecked-long (ibuf idx)))
                                                           false true)))
                                    (= :string dtype)
                                    (str-data->coldata data)
                                    (= :text dtype)
                                    (text-data->coldata data)
                                    (= :local-date dtype)
                                    (-> (b64->numeric-data data :int32)
                                        (dtype/->array-buffer)
                                        (abuf/set-datatype :packed-local-date))
                                    (= :instant dtype)
                                    (-> (b64->numeric-data data :int64)
                                        (dtype/->array-buffer)
                                        (abuf/set-datatype :packed-instant))
                                    :else
                                    (dtype/make-container dtype data))
                                 :name            (:name metadata)})))
         (ds-impl/new-dataset {:dataset-name (:name ds-meta)} ds-meta))))

(comment
  ;; verify that metadata is preserved
  (let [ds_a (ds/->dataset {:a [5]} {:parser-fn {:a :uint8} :dataset-name "keep-this-name"})
        ds_b (-> ds_a dataset->data data->dataset)]
    (and (= ds_a ds_b)
         (= (meta ds_a) (meta ds_b))))
  )

(def ^{:doc "Transit handler for writing datasets."}
  write-handlers {Dataset (t/write-handler "tech.v3.dataset" dataset->data)})
(def ^{:doc "Transit handler for reading datasets."}
  read-handlers {"tech.v3.dataset" (t/read-handler data->dataset)})

(def ^{:doc "Transit write handlers for java.time.LocalDate and java.time.Instant"}
  java-time-write-handlers
  {LocalDate (t/write-handler "java.time.LocalDate" dtype-dt/local-date->days-since-epoch)
   Instant (t/write-handler "java.time.Instant" dtype-dt/instant->microseconds-since-epoch)})

(def ^{:doc "Transit read handlers for java.time.LocalDate and java.time.Instant"}
  java-time-read-handlers
  {"java.time.LocalDate" (t/read-handler dtype-dt/days-since-epoch->local-date)
   "java.time.Instant" (t/read-handler dtype-dt/microseconds-since-epoch->instant)})


(defn dataset->transit
  "Convert a dataset into a transit encoded writer. See source for details."
  [ds out & [format handlers]]
  (let [writer (t/writer out (or format :json) {:handlers (merge write-handlers handlers)})]
    (t/write writer ds)))


(defn dataset->transit-str
  "Convert a dataset to a transit-encoded json string.  See [[dataset->transit]]."
  [ds & [format handlers]]
  (let [out (java.io.ByteArrayOutputStream.)]
    (dataset->transit ds out format handlers)
    (String. (.toByteArray out))))


(defn transit->dataset
  "Decode a transite reader installing the transit handlers for datasets.  See source
  for details."
  [in & [format handlers]]
  (let [reader (t/reader in (or format :json) {:handlers (merge read-handlers handlers)})]
    (t/read reader)))


(defn transit-str->dataset
  "Convert a transit string to a dataset.  This simply decodes the string, it isn't
  dataset specific.  See [[transit->dataset]]"
  [ds-str]
  (transit->dataset (java.io.ByteArrayInputStream. (.getBytes ^String ds-str))))


(defmethod ds-io/data->dataset :transit-json
  [data options]
  (with-open [in (io/input-stream data)]
    (transit->dataset in (:transit-format options)
                      (:transit-read-handlers options))))


(defmethod ds-io/dataset->data! :transit-json
  [ds output options]
  (with-open [outs (io/output-stream! output)]
    (dataset->transit ds outs
                      (:transit-format options)
                      (:transit-write-handlers options))))

(comment
  (defn master-ds
    []
    (ds/->dataset (array-map :a (mapv double (range 5))
                             :b (repeat 5 :a)
                             :c (repeat 5 "hey")
                             :d (repeat 5 {:a 1 :b 2})
                             :e (repeat 4 [1 2 3])
                             :f (repeat 5 (dtype-dt/local-date))
                             :g (repeat 5 (dtype-dt/instant))
                             :h [true false true true false]
                             :i (repeat 5 "text")
                             :j [1 nil 2 nil 3])
                  {:parser-fn {:i :text}}))
  


  (-> (master-ds)
      (dataset->transit-str)
      (transit-str->dataset))


  (def test-data (vec (repeatedly 100000 #(hash-map :time (rand)
                                                    :temp (rand)
                                                    :valid? (if (> (rand) 0.5)
                                                              true
                                                              false)))))

  (def test-ds (ds/->dataset test-data))

  (do

    (defn t->file
      [ds fname]
      (with-open [outs (io/output-stream! fname)]
        (dataset->transit ds outs)))

    (defn file->t
      [fname]
      (with-open [ins (io/input-stream fname)]
        (transit->dataset ins)))
    )

  (time (t->file test-data "mapseq.transit-json"))
  ;; 1027ms, 6.3MB raw, 1.9MB gzipped

  (time (t->file test-ds "ds.transit-json"))
  ;;   16ms, 2.2MB raw, 1.6MB gzipped

  (do
    (def stocks (ds/->dataset "https://github.com/techascent/tech.ml.dataset/raw/master/test/data/stocks.csv" {:key-fn keyword}))

    (io/make-parents "test/data/stocks.transit-json")
    (t->file stocks "test/data/stocks.transit-json")
    )

  (require '[tech.v3.datatype.functional :as dfn])

  (->> (ds/group-by-column stocks :symbol)
       (map (fn [[k v]]
              [k (-> (dfn/mean (v :price))
                     (* 100)
                     (Math/round)
                     (/ 100.0))]))
       (into {}))
  ;;{"MSFT" 24.74, "AMZN" 47.99, "IBM" 91.26, "GOOG" 415.87, "AAPL" 64.73}


  )
