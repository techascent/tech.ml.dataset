(ns tech.v3.dataset.impl.column-base
  (:require [tech.v3.dataset.string-table :as str-table]
            [tech.v3.dataset.file-backed-text :as file-backed-text]
            [tech.v3.datatype.datetime :as dtype-dt]
            [tech.v3.datatype.packing :as packing]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype :as dtype]
            [clojure.tools.logging :as log])
  (:import [java.util Map List]
           [tech.v3.datatype PrimitiveList]
           [tech.v3.dataset Text]))


(defn- pack-nil
  ^long [unpacked-datatype]
  (let [unpacked-container (dtype/make-container unpacked-datatype [nil])
        packed-container (packing/pack unpacked-container)]
    (-> (dtype/as-buffer packed-container)
        (.readLong 0))))


(def ^Map dtype->missing-val-map
  {:boolean false
   :int8 Byte/MIN_VALUE
   :int16 Short/MIN_VALUE
   :int32 Integer/MIN_VALUE
   :int64 Long/MIN_VALUE
   :float32 Float/NaN
   :float64 Double/NaN
   :packed-instant (pack-nil :instant)
   :packed-local-date (pack-nil :local-date)
   :packed-duration (pack-nil :duration)
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


(defn datatype->packed-missing-value
  [dtype]
  (get dtype->missing-val-map dtype
       (when (casting/numeric-type? dtype)
         (casting/cast 0 dtype))))


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
