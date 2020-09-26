(ns tech.v3.dataset.parse.base
  (:require [tech.v3.dataset.parse.datetime :as parse-dt]
            [tech.v3.datatype.packing :as packing]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.bitmap :as bitmap]
            [tech.v3.datatype.errors :as errors])
  (:import [java.util UUID]
           [tech.v3.datatype PrimitiveList]
           [org.roaringbitmap RoaringBitmap]))


(set! *warn-on-reflection* true)


;;For sequences of maps and spreadsheets
;; 1. No parser specified -  do not change datatype, do not change input value.
;; Promote container as necessary; promotion requires a static cast when possible or
;; all the way to object.
;; 2. datatype,parser specified - if input value is correct datatype, add to container.
;;    if input is incorrect datatype, call parse fn.


;;For string-based pathways
;; 1. If no parser is specified, try-parse where ::parse-failure means
;; 2. If parse is specified, use parse if


(defmacro dtype->parse-fn
  [datatype val]
  (case datatype
    :boolean `(boolean
               (cond
                 (or (.equalsIgnoreCase "t" ~val)
                     (.equalsIgnoreCase "y" ~val)
                     (.equalsIgnoreCase "yes" ~val)
                     (.equalsIgnoreCase "True" ~val))
                 true
                 (or (.equalsIgnoreCase "f" ~val)
                     (.equalsIgnoreCase "n" ~val)
                     (.equalsIgnoreCase "no" ~val)
                     (.equalsIgnoreCase "false" ~val))
                 false
                 :else
                 (throw (Exception. (format "Boolean parse failure: %s" ~val) ))))
    :int16 `(Short/parseShort ~val)
    :int32 `(Integer/parseInt ~val)
    :int64 `(Long/parseLong ~val)
    :float32 `(Float/parseFloat ~val)
    :float64 `(Double/parseDouble ~val)
    :uuid `(UUID/fromString ~val)
    :keyword `(keyword ~val)
    :symbol `(symbol ~val)))


(def parse-failure :tech.ml.dataset.parse/parse-failure)
(def missing :tech.ml.dataset.parse/missing)

(def str-parsers
  (merge
   {:boolean #(let [^String data %]
                (boolean
                 (cond
                   (or (.equalsIgnoreCase "t" data)
                       (.equalsIgnoreCase "y" data)
                       (.equalsIgnoreCase "yes" data)
                       (.equalsIgnoreCase "True" data))
                   true
                   (or (.equalsIgnoreCase "f" data)
                       (.equalsIgnoreCase "n" data)
                       (.equalsIgnoreCase "no" data)
                       (.equalsIgnoreCase "false" data))
                   false
                   :else
                   parse-failure)))
    :int16 #(try (Short/parseShort %) (catch Throwable e parse-failure))
    :int32 #(try (Integer/parseInt %) (catch Throwable e parse-failure))
    :int64 #(try (Long/parseLong %) (catch Throwable e parse-failure))
    :float32 #(try (Float/parseFloat %) (catch Throwable e parse-failure))
    :float64 #(try (Double/parseDouble %) (catch Throwable e parse-failure))
    :uuid #(try (UUID/fromString %) (catch Throwable e parse-failure))
    :keyword #(if-let [retval (keyword %)]
                retval
                parse-failure)
    :symbol #(if-let [retval (symbol %)]
               retval
               parse-failure)}
   (->> parse-dt/datatype->general-parse-fn-map
        (mapcat (fn [[k v]]
                  (let [unpacked-parser #(try (v %)
                                              (catch Throwable e parse-failure))]
                    (if (packing/unpacked-datatype? k)
                      [[k unpacked-parser]
                       [(packing/pack-datatype k)
                        #(try (packing/pack (v %))
                              (catch Throwable e parse-failure))]]
                      [[k unpacked-parser]]))))
        (into {}))))


(defprotocol PParser
  (add-value! [p idx value])
  (finalize! [p rowcount]))


(defn add-missing-values!
  [^PrimitiveList container ^RoaringBitmap missing
   missing-value ^long idx]
  (let [n-elems (.lsize container)]
    (loop [n-elems n-elems]
      (when (< n-elems idx)
        (.addObject container missing-value)
        (.add missing n-elems)
        (recur (unchecked-inc n-elems))))))


(deftype FixedTypeParser [^PrimitiveList container
                          container-dtype
                          ^RoaringBitmap missing
                          missing-value parse-fn
                          ^PrimitiveList failed-values]
  PParser
  (add-value! [p idx value]
    (when-not (= (long idx) (.lsize container))
      (add-missing-values! container missing missing-value idx))
    (if (= (dtype/elemwise-datatype value)
           container-dtype)
      (.add container value)
      (let [parsed-value (parse-fn value)]
        (cond
          (= parsed-value parse-failure)
          (if failed-values
            (do
              (.addObject failed-values value)
              (.add missing (.size container))
              (.addObject container missing-value))
            (errors/throwf "Failed to parse value %s as datatype %s" value container-dtype))
          (= parsed-value :missing)
          (do
            (.add missing (.size container))
            (.addObject container missing-value))
          :else
          (.addObject container parsed-value)))))
  (finalize! [p rowcount]
    (add-missing-values! container missing missing-value rowcount)
    (merge
     {:data container
      :missing missing
      :force-datatype? true}
     (when failed-values
       {:metadata {:unparsed-data failed-values}}))))
