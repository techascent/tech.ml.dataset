(ns tech.v3.dataset.compress
  (:require [ham-fisted.api :as hamf]
            [tech.v3.datatype :as dt]
            [tech.v3.datatype.protocols :as dt-proto]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.packing :as packing]
            [tech.v3.dataset.protocols :as ds-proto]
            [tech.v3.dataset.impl.column :as ds-col]
            [ham-fisted.print :refer [implement-tostring-print]])
  (:import [ham_fisted IMutList ArrayLists]
           [tech.v3.datatype LongBuffer DoubleBuffer ObjectBuffer Buffer]
           [java.util Map Set]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(definterface ToMap (^java.util.Map toMap []))

(defn make-int-buffer [buf dt]
  (case dt
    :int8 (hamf/byte-array buf)
    :int16 (hamf/short-array buf)
    :int32 (hamf/int-array buf)
    :int64 (hamf/long-array buf)))

(deftype OffsetBuffer [^long offset dtype ^long n-elems data]
  dt-proto/PECount (ecount [_] n-elems)
  dt-proto/PElemwiseDatatype (elemwise-datatype [_] dtype)
  dt-proto/PSubBuffer (sub-buffer [_ off len]
                        (let [off (long off) len (long len)
                              ^IMutList ll (-> (ArrayLists/toList data)
                                               (.subList off (+ off len)))]
                          (OffsetBuffer. offset dtype (long (.size ll)) (.toNativeArray ll))))
  dt-proto/PToBuffer
  (convertible-to-buffer? [_] true)
  (->buffer [_]
    (let [mm (ArrayLists/toList data)
          rv (case dtype
               :int8 (reify LongBuffer
                       (elemwiseDatatype [_] dtype)
                       (lsize [_] n-elems)
                       (readLong [_ idx] (+ offset (.getLong mm idx)))
                       (readObject [t idx] (unchecked-byte (.readLong t idx))))
               (:int16 :uint8) (reify LongBuffer
                                 (elemwiseDatatype [_] dtype)
                                 (lsize [_] n-elems)
                                 (readLong [_ idx] (+ offset (.getLong mm idx)))
                                 (readObject [t idx] (unchecked-short (.readLong t idx))))
               (:int32 :uint16) (reify LongBuffer
                                  (elemwiseDatatype [_] dtype)
                                  (lsize [_] n-elems)
                                  (readLong [_ idx] (+ offset (.getLong mm idx)))
                                  (readObject [t idx] (unchecked-int (.readLong t idx))))
               (reify LongBuffer
                 (elemwiseDatatype [_] dtype)
                 (lsize [_] n-elems)
                 (readLong [_ idx] (+ offset (.getLong mm idx)))
                 (readObject [t idx] (.readLong t idx))))]
      (if (packing/packed-datatype? dtype)
        (packing/unpack rv)
        rv)))
  ToMap
  (toMap [_] {:compressed-buffer-type :offset-buffer :offset offset :dtype dtype :n-elems n-elems :data data
              :data-dtype (dt/elemwise-datatype data)})
  Object
  (toString [t] (.toString (dt/->reader t)))
  (hashCode [t] (.hashCode (dt/->reader t)))
  (equals [t o] (.equals (dt/->reader t) o)))

(implement-tostring-print OffsetBuffer)

(defn offset-buffer [offset dtype n-elems data] (OffsetBuffer. offset dtype n-elems data))
(defn map->OffsetBuffer [m] (OffsetBuffer. (:offset m) (:dtype m) (:n-elems m)
                                           (make-int-buffer (:data m) (:data-dtype m))))

(defn offset-buffer? [b] (instance? OffsetBuffer b))

(deftype ConstBuffer [^long n-elems dtype v]
  dt-proto/PECount (ecount [_] n-elems)
  dt-proto/PElemwiseDatatype (elemwise-datatype [_] dtype)
  dt-proto/PSubBuffer (sub-buffer [_ off len]
                        (let [off (long off) len (long len)]
                          (ham_fisted.ChunkedList/sublistCheck off (+ off len) n-elems)
                          (ConstBuffer. (- len off) dtype v)))
  ToMap
  (toMap [_] {:compressed-buffer-type :const-buffer :n-elems n-elems :dtype dtype :v v})
  dt-proto/PToBuffer
  (convertible-to-buffer? [_] true)
  (->buffer [_] (dt/const-reader v n-elems))
  Object
  (toString [t] (.toString ^Object (dt/->reader t)))
  (hashCode [t] (.hashCode ^Object (dt/->reader t)))
  (equals [t o] (.equals ^Object (dt/->reader t) o)))

(implement-tostring-print ConstBuffer)

(defn const-buffer [n-elems dtype v] (ConstBuffer. n-elems dtype v))
(defn map->ConstBuffer [m] (ConstBuffer. (:n-elems m) (:dtype m) (:v m)))

(defn const-buffer? [b] (instance? ConstBuffer b))

(deftype DictBuffer [dict dtype ^long n-elems indexes]
  dt-proto/PECount (ecount [_] n-elems)
  dt-proto/PElemwiseDatatype (elemwise-datatype [_] dtype)
  dt-proto/PSubBuffer (sub-buffer [_ off len]
                        (let [off (long off) len (long len)
                              _ (ham_fisted.ChunkedList/sublistCheck off (+ off len) n-elems)
                              ^IMutList idx (-> (ArrayLists/toList indexes)
                                                (.subList off (+ off len)))]
                          (DictBuffer. dict dtype len (.toNativeArray idx))))
  dt-proto/PToBuffer
  (convertible-to-buffer? [_] true)
  (->buffer [_]
    (let [dict (ArrayLists/toList dict)
          indexes (ArrayLists/toList indexes)]
      (cond
        (casting/integer-type? dtype)
        (reify LongBuffer
          (lsize [_] n-elems)
          (elemwiseDatatype [_] dtype)
          (readLong [_ idx] (.getLong dict (.getLong indexes idx)))
          (readObject [t idx] (casting/cast (.readLong t idx) dtype)))
        (casting/float-type? dtype)
        (reify DoubleBuffer
          (lsize [_] n-elems)
          (elemwiseDatatype [_] dtype)
          (readDouble [_ idx] (.getDouble dict (.getDouble indexes idx)))
          (readObject [t idx] (casting/cast (.readDouble t idx) dtype)))
        :else
        (reify ObjectBuffer
          (lsize [_] n-elems)
          (elemwiseDatatype [_] dtype)
          (readObject [_ idx] (.get dict (.getLong indexes idx)))))))
  ToMap
  (toMap [_] {:compressed-buffer-type :dict-buffer :dict dict :n-elems n-elems :dtype dtype :indexes indexes
              :index-dtype (dt/elemwise-datatype indexes)})
  Object
  (toString [t] (.toString ^Object (dt/->reader t)))
  (hashCode [t] (.hashCode ^Object (dt/->reader t)))
  (equals [t o] (.equals ^Object (dt/->reader t) o)))

(implement-tostring-print DictBuffer)

(defn dict-buffer [dict dtype n-elems indexes] (DictBuffer. dict dtype n-elems indexes))
(defn map->DictBuffer [m] (DictBuffer. (hamf/object-array (:dict m)) (:dtype m) (:n-elems m)
                                       (make-int-buffer (:index-dtype m) (:indexes m))))

(defn dict-buffer? [m] (instance? DictBuffer m))

(defn compressed-buffer->map
  [bb]
  (.toMap ^ToMap bb))

(defn map->compressed-buffer
  [m]
  (case (:compressed-buffer-type m)
    :offset-buffer (map->OffsetBuffer m)
    :const-buffer (map->ConstBuffer m)
    :dict-buffer (map->DictBuffer m)))

(defn compressed-buffer?
  [m]
  (or (offset-buffer? m) (const-buffer? m) (dict-buffer? m)))

(defn reader->byte-array [^long offset ^Buffer rdr]
  (let [rv (byte-array (.size rdr))]
    (dotimes [idx (alength rv)]
      (aset rv idx (unchecked-byte (- (.readLong rdr idx) offset))))
    rv))

(defn reader->short-array [^long offset ^Buffer rdr]
  (let [rv (short-array (.size rdr))]
    (dotimes [idx (alength rv)]
      (aset rv idx (unchecked-short (- (.readLong rdr idx) offset))))
    rv))

(defn reader->int-array [^long offset ^Buffer rdr]
  (let [rv (int-array (.size rdr))]
    (dotimes [idx (alength rv)]
      (aset rv idx (unchecked-int (- (.readLong rdr idx) offset))))
    rv))

(defn compress-integer-buffer
  [n-elems dtype ocbuf cbuf]
  (let [{:keys [^long max ^long min]} (hamf/lsummary cbuf)
        mdiff (- max min)]
    (cond
      (== max min) (const-buffer n-elems dtype (casting/cast min dtype))
      (and (<= mdiff 127) (not (or (identical? dtype :int8) (identical? dtype :uint8))))
      (offset-buffer min dtype n-elems (reader->byte-array min ocbuf))
      (and (<= mdiff 32767) (not (or (identical? dtype :int16) (identical? dtype :uint16))))
      (offset-buffer min dtype n-elems (reader->short-array min ocbuf))
      (and (<= mdiff 2147483647) (not (or (identical? dtype :int32) (identical? dtype :uint32))))
      (offset-buffer min dtype n-elems (reader->int-array min ocbuf)))))

(defn compress-double-buffer
  [n-elems dtype cbuf]
  (let [{:keys [^double max ^double min]} (hamf/dsummary cbuf)]
    (when (== max min)
      (const-buffer n-elems dtype (casting/cast min dtype)))))

(defn compress-object-buffer
  [n-elems dtype ocbuf & {:as opts}]
  (let [hs (hamf/mut-map {nil 0})
        dict (hamf/object-array-list)
        indexes (hamf/long-array-list)
        max-dict-ratio (double (get opts :max-dict-ratio 0.50))
        use-dict? (reduce (fn [_ vv]
                            (let [idx (.size dict)
                                  map-idx (.putIfAbsent hs vv idx)
                                  _ (do
                                      (when-not map-idx (.add dict vv))
                                      (.addLong indexes (long (or map-idx idx))))
                                  dict-ratio (/ (.size dict) (double n-elems))]
                              (if (> dict-ratio max-dict-ratio)
                                (reduced false)
                                true)))
                          true ocbuf)]
    (when (and (not (.isEmpty dict))  use-dict?)
      (if (== 1 (.size dict))
        (const-buffer n-elems dtype (.get dict 0))
        (let [n-dict (.size dict)]
          (dict-buffer (.toArray dict) dtype n-elems
                       (cond (<= n-dict 127)
                             (hamf/byte-array indexes)
                             (<= n-dict 32767)
                             (hamf/short-array indexes)
                             (<= n-dict 2147483647)
                             (hamf/int-array indexes)
                             :else
                             (hamf/long-array indexes))))))))

(defn nonmissing-buffer [col ocbuf]
  (if (ds-proto/any-missing? col)
    (dt/indexed-buffer (ds-proto/valid-rows col) ocbuf)
    ocbuf))

(defn compress-column-buffer
  [col & {:as opts}]
  (let [n-elems (dt/ecount col)
        ocbuf (dt/->reader col)
        dtype (dt/elemwise-datatype col)]
    (case (casting/simple-operation-space dtype)
      :int64 (compress-integer-buffer n-elems dtype ocbuf (nonmissing-buffer col ocbuf))
      :float64 (compress-double-buffer n-elems dtype (nonmissing-buffer col ocbuf))
      (compress-object-buffer n-elems dtype ocbuf opts))))

(defn compress-column
  [col & {:as opts}]
  (let [compressed (compress-column-buffer col opts)]
    (if compressed
      (ds-col/construct-column (ds-proto/missing col) compressed (meta col))
      col)))

(defn compress-ds
  [ds & {:as opts}]
  (when ds
    (->> (.values ^Map ds)
         (hamf/pmap #(compress-column % opts))
         (reduce (fn [ds col]
                   (assoc ds (ds-proto/column-name col) col))
                 ds))))

(comment
  (require '[tech.v3.dataset :as ds])

  (def ds (ds/->dataset {:a (-> (vec (range 100))
                                (assoc 1 nil 30 nil))
                         :b (-> (vec (repeat 100 :a))
                                (assoc 3 nil 98 nil))
                         :c (take 100 (cycle [:a :b :c]))
                         :d (vec (tech.v3.datatype.datetime.operations/plus-temporal-amount
                                  (dt/make-container :packed-local-date (repeat 100 (java.time.LocalDate/now)))
                                  (range 100)
                                  :days))}))
  (def cds (compress-ds ds))
  :-)
