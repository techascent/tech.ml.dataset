(ns tech.v3.dataset.impl.column-base
  (:require [tech.v3.dataset.string-table :as str-table]
            [tech.v3.dataset.file-backed-text :as file-backed-text]
            [tech.v3.datatype.datetime :as dtype-dt]
            [tech.v3.datatype.packing :as packing]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.argops :as argops]
            [tech.v3.datatype :as dtype]
            [clojure.tools.logging :as log])
  (:import [java.util Map List Comparator]
           [java.util.function Consumer]
           [tech.v3.datatype PrimitiveList ECount]
           [tech.v3.dataset Text]
           [clojure.lang IDeref]))


(def ^Map dtype->missing-val-map
  {:boolean false
   :int8 Byte/MIN_VALUE
   :int16 Short/MIN_VALUE
   :int32 Integer/MIN_VALUE
   :int64 Long/MIN_VALUE
   :float32 Float/NaN
   :float64 Double/NaN
   :packed-instant (packing/pack (dtype-dt/milliseconds-since-epoch->instant 0))
   :packed-local-date (packing/pack (dtype-dt/milliseconds-since-epoch->local-date 0))
   :packed-duration 0
   :instant nil
   :zoned-date-time nil
   :local-date-time nil
   :local-date nil
   :local-time nil
   :duration nil
   :string ""
   :text nil
   :keyword nil
   :symbol nil})


(casting/add-object-datatype! :text Text)


(defn datatype->missing-value
  [dtype]
  (let [packed? (packing/packed-datatype? dtype)
        dtype (if packed?
                (packing/unpack-datatype dtype)
                (casting/un-alias-datatype dtype))]
    (get dtype->missing-val-map dtype
         (when (casting/numeric-type? dtype)
           (casting/cast 0 dtype)))))


(defonce ^:private warn-atom* (atom false))
(defonce file-backed-text-enabled* (atom true))

(defn set-file-backed-text-enabled
  [enabled]
  (reset! file-backed-text-enabled* enabled)
  enabled)

(defn make-container
  (^PrimitiveList [dtype options]
   (case dtype
     :string (str-table/make-string-table 0 "")
     :text
     (let [^PrimitiveList list-data
           (try
             (if (and (not= false (:text-temp-dir options))
                      @file-backed-text-enabled*)
               (let [tmp-dir (:text-temp-dir options)]
                 (file-backed-text/file-backed-text (merge
                                                     {:suffix ".txt"}
                                                     (when tmp-dir
                                                       {:temp-dir tmp-dir}))))
               (dtype/make-list :text))
             (catch Throwable e
               (when-not @warn-atom*
                 (reset! warn-atom* true)
                 (log/warn e "File backed text failed.  Falling back to in-memory"))
               (dtype/make-list :text)))]
             list-data)
     (dtype/make-list dtype)))
  (^PrimitiveList [dtype]
   (make-container dtype nil)))


(defn column-datatype-categorical?
  "Anything where we don't know the conversion to a scalar double or integer
  number is considered automatically categorical."
  [col-dtype]
  (and (not (casting/numeric-type? col-dtype))
       (not (identical? col-dtype :boolean))
       (not (dtype-dt/datetime-datatype? col-dtype))))


(defn- value-order
  [prev-val next-val ^Comparator comparator]
  (let [comp (.compare comparator prev-val next-val)]
    (if (== comp 0)
      :tech.numerics/==
      (if (> comp 0)
        :tech.numerics/>
        :tech.numerics/<))))


(defn- value-order-long
  [^long prev-val ^long next-val]
  (let [comp (Long/compare prev-val next-val)]
    (if (== comp 0)
      :tech.numerics/==
      (if (> comp 0)
        :tech.numerics/>
        :tech.numerics/<))))


(defn- value-order-double
  [^double prev-val ^double next-val]
  (let [comp (Double/compare prev-val next-val)]
    (if (== comp 0)
      :tech.numerics/==
      (if (> comp 0)
        :tech.numerics/>
        :tech.numerics/<))))


(deftype ColumnStatistics [^:unsynchronized-mutable min-value
                           ^:unsynchronized-mutable max-value
                           ^:unsynchronized-mutable last-value
                           ^:unsynchronized-mutable order
                           ^{:unsynchronized-mutable true
                             :tag long} n-elems
                           cast-fn
                           user-comparator
                           ^Comparator comparator]
  Consumer
  (accept [this val]
    ;;overshadow val of val with correct type
    (let [val (cast-fn val)]
      (if (== 0 n-elems )
        (do (set! min-value val)
            (set! max-value val)
            (set! order :tech.numerics/==))
        (let [new-order (value-order last-value val comparator)]
          (when-not (identical? new-order order)
            (if (== n-elems 1)
              (set! order new-order)
              (set! order :tech.numerics/unordered)))
          (let [max-comp (.compare comparator max-value val)
                min-comp (.compare comparator min-value val)]
            (when (< max-comp 0)
              (set! max-value val))
            (when (> min-comp 0)
              (set! min-value val)))))
      (set! n-elems (unchecked-inc n-elems))
      (set! last-value val)))
  ECount
  (lsize [this] n-elems)
  IDeref
  (deref [this]
    (when-not (== 0 n-elems)
      (merge
       {:min min-value
        :max max-value
        :order order}
       (when-not (identical? user-comparator :tech.numerics/<)
         {:comparator user-comparator})))))


(deftype ColumnStatisticsLong [^{:unsynchronized-mutable true
                                 :tag long} min-value
                               ^{:unsynchronized-mutable true
                                 :tag long} max-value
                               ^{:unsynchronized-mutable true
                                 :tag long} last-value
                               ^:unsynchronized-mutable order
                               ^{:unsynchronized-mutable true
                                 :tag long} n-elems]
  Consumer
  (accept [this val]
    ;;overshadow val of val with correct type
    (let [val (unchecked-long val)]
      (if (== 0 n-elems )
        (do (set! min-value val)
            (set! max-value val)
            (set! order :tech.numerics/==))
        (let [new-order (value-order-long last-value val)]
          (when-not (identical? new-order order)
            (if (== n-elems 1)
              (set! order new-order)
              (set! order :tech.numerics/unordered)))
          (when (> val max-value)
            (set! max-value val))
          (when (< val min-value)
            (set! min-value val))))
      (set! n-elems (unchecked-inc n-elems))
      (set! last-value val)))
  ECount
  (lsize [this] n-elems)
  IDeref
  (deref [this]
    (when-not (== 0 n-elems)
      (merge
       {:min min-value
        :max max-value
        :order order}))))


(deftype ColumnStatisticsDouble [^{:unsynchronized-mutable true
                                   :tag double} min-value
                                 ^{:unsynchronized-mutable true
                                   :tag double} max-value
                                 ^{:unsynchronized-mutable true
                                   :tag double} last-value
                                 ^:unsynchronized-mutable order
                                 ^{:unsynchronized-mutable true
                                   :tag long} n-elems]
  Consumer
  (accept [this val]
    ;;overshadow val of val with correct type
    (let [val (unchecked-double val)]
      (if (== 0 n-elems )
        (do (set! min-value val)
            (set! max-value val)
            (set! order :tech.numerics/==))
        (let [new-order (value-order-double last-value val)]
          (when-not (identical? new-order order)
            (if (== n-elems 1)
              (set! order new-order)
              (set! order :tech.numerics/unordered)))
          (when (> val max-value)
            (set! max-value val))
          (when (< val min-value)
            (set! min-value val))))
      (set! n-elems (unchecked-inc n-elems))
      (set! last-value val)))
  ECount
  (lsize [this] n-elems)
  IDeref
  (deref [this]
    (when-not (== 0 n-elems)
      (merge
       {:min min-value
        :max max-value
        :order order}))))


(defn column-statistics-datatype?
  [dtype]
  (or (and (not (identical? dtype :char))
           (casting/numeric-type? dtype))
      (dtype-dt/datetime-datatype? dtype)))


(defn column-statistics-consumer
  (^Consumer [dtype options]
   (let [user-comparator (:comparator options :tech.numerics/<)
         dtype (-> (packing/unpack-datatype dtype)
                   (casting/simple-operation-space))
         comparator (-> (argops/find-base-comparator
                         user-comparator dtype)
                        (argops/->comparator))]
     (case dtype
       :int64 (ColumnStatisticsLong. Long/MIN_VALUE Long/MIN_VALUE Long/MIN_VALUE
                                     :tech.v3.dataset/unordered 0)
       :float64 (ColumnStatisticsDouble. Double/NaN Double/NaN Double/NaN
                                         :tech.v3.dataset/unordered 0)
       ;;default case
       (ColumnStatistics. nil nil nil :tech.v3.dataset/unordered 0
                          identity
                          (when (keyword? user-comparator) user-comparator)
                          comparator))))
  (^Consumer [dtype]
   (column-statistics-consumer dtype nil)))
