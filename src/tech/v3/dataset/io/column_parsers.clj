(ns tech.v3.dataset.io.column-parsers
  "Per-column parsers."
  (:require [tech.v3.dataset.io.datetime :as parse-dt]
            [tech.v3.dataset.impl.column-base :as column-base]
            [tech.v3.datatype.packing :as packing]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.bitmap :as bitmap]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.argtypes :refer [arg-type]]
            [tech.v3.datatype.argops :as argops]
            [tech.v3.datatype.datetime :as dtype-dt]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.dataset.impl.dataset :as ds-impl])
  (:import [java.util UUID List HashMap]
           [java.util.function Function]
           [tech.v3.datatype PrimitiveList]
           [org.roaringbitmap RoaringBitmap]
           [clojure.lang IFn]
           [java.time.format DateTimeFormatter]))


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


(def parse-failure :tech.ml.dataset.parse/parse-failure)
(def missing :tech.ml.dataset.parse/missing)

(defn make-safe-parse-fn
  [parser-fn]
  (fn [str-val]
    (try
      (parser-fn str-val)
      (catch Throwable e
        parse-failure))))

(defn- packed-parser
  [datatype str-parse-fn]
  (make-safe-parse-fn (packing/wrap-with-packing
                       datatype
                       #(if (= datatype (dtype/elemwise-datatype %))
                          %
                          (str-parse-fn %)))))

(def default-coercers
  (merge
   {:boolean #(if (string? %)
                (let [^String data %]
                  (cond
                    (or (.equalsIgnoreCase "t" data)
                        (.equalsIgnoreCase "y" data)
                        (.equalsIgnoreCase "yes" data)
                        (.equalsIgnoreCase "True" data)
                        (.equalsIgnoreCase "positive" data))
                    true
                    (or (.equalsIgnoreCase "f" data)
                        (.equalsIgnoreCase "n" data)
                        (.equalsIgnoreCase "no" data)
                        (.equalsIgnoreCase "false" data)
                        (.equalsIgnoreCase "negative" data))
                    false
                    :else
                    parse-failure))
                (boolean %))
    :int16 (make-safe-parse-fn #(if (string? %)
                                  (Short/parseShort %)
                                  (short %)))
    :int32 (make-safe-parse-fn  #(if (string? %)
                                   (Integer/parseInt %)
                                   (int %)))
    :int64 (make-safe-parse-fn #(if (string? %)
                                  (Long/parseLong %)
                                  (long %)))
    :float32 (make-safe-parse-fn #(if (string? %)
                                    (let [fval (Float/parseFloat %)]
                                      (if (Float/isNaN fval)
                                        missing
                                        fval))
                                    (float %)))
    :float64 (make-safe-parse-fn #(if (string? %)
                                    (let [dval (Double/parseDouble %)]
                                      (if (Double/isNaN dval)
                                        missing
                                        dval))
                                    (double %)))
    :uuid (make-safe-parse-fn #(if (string? %)
                                 (UUID/fromString %)
                                 (if (instance? UUID %)
                                   %
                                   parse-failure)))
    :keyword #(if-let [retval (keyword %)]
                retval
                parse-failure)
    :symbol #(if-let [retval (symbol %)]
               retval
               parse-failure)
    :string #(let [str-val (or (if (string? %)
                                 %
                                 (str %)))]
               (if (< (count str-val) 1024)
                 str-val
                 parse-failure))
    :text #(str %)}
   (->> parse-dt/datatype->general-parse-fn-map
        (mapcat (fn [[k v]]
                  (let [unpacked-parser (make-safe-parse-fn
                                         #(if (= k (dtype/elemwise-datatype %))
                                            %
                                            (v %)))]
                    ;;packing is now done at the container level.
                    (if (packing/unpacked-datatype? k)
                      [[k unpacked-parser]
                       [(packing/pack-datatype k) unpacked-parser]]
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


(defn finalize-parser-data!
  [container missing failed-values failed-indexes
   missing-value rowcount]
  (add-missing-values! container missing missing-value rowcount)
  (merge
   {:data container
    :missing missing
      :force-datatype? true}
   (when (and failed-values
                (not= 0 (dtype/ecount failed-values)))
     {:metadata {:unparsed-data failed-values
                 :unparsed-indexes failed-indexes}})))


(defn- missing-value?
  [^String value]
  (or (nil? value)
      (= "" value)
      (and (string? value) (.equalsIgnoreCase value "na"))))


(defn- not-missing?
  [parsed-value missing-value]
  (or (not= parsed-value missing-value)
      (= parsed-value false)))


(deftype FixedTypeParser [^PrimitiveList container
                          container-dtype
                          missing-value parse-fn
                          ^RoaringBitmap missing
                          ^PrimitiveList failed-values
                          ^RoaringBitmap failed-indexes
                          column-name]
  dtype-proto/PECount
  (ecount [this] (.lsize container))
  PParser
  (add-value! [p idx value]
    (when-not (missing-value? value)
      (let [idx (long idx)]
        (let [value-dtype (dtype/elemwise-datatype value)]
          (if (= value-dtype container-dtype)
            (do
              (add-missing-values! container missing missing-value idx)
              (.add container value))
            (let [parsed-value (parse-fn value)]
              (cond
                (= parsed-value parse-failure)
                (if failed-values
                  (do
                    (.addObject failed-values value)
                    (.add failed-indexes (unchecked-int idx)))
                  (errors/throwf "Failed to parse value %s as datatype %s on row %d"
                                 value container-dtype idx))
                (not-missing? parsed-value missing-value)
                (do
                  (add-missing-values! container missing missing-value idx)
                  (.addObject container parsed-value)))))))))
  (finalize! [p rowcount]
    (finalize-parser-data! container missing failed-values failed-indexes
                           missing-value rowcount)))


(defn- find-fixed-parser
  [kwd]
  (if (or (= kwd :string)
          (= kwd :text))
    str
    (if-let [retval (get default-coercers kwd)]
      retval
      (errors/throwf "Failed to find parser for keyword %s" kwd))))


(defn- datetime-formatter-parser-fn
  [parser-datatype formatter]
  (let [unpacked-datatype (packing/unpack-datatype parser-datatype)
        parser-fn (parse-dt/datetime-formatter-parse-str-fn
                   unpacked-datatype formatter)]
    [(make-safe-parse-fn
      (packing/wrap-with-packing
       parser-datatype parser-fn)) false]))


(defn parser-entry->parser-tuple
  [parser-kwd]
  (if (vector? parser-kwd)
    (do
      (assert (= 2 (count parser-kwd)))
      (let [[parser-datatype parser-fn] parser-kwd]
        (assert (keyword? parser-datatype))
        [parser-datatype
         (cond
           (= :relaxed? parser-fn)
           [(find-fixed-parser parser-datatype) true]
           (instance? IFn parser-fn)
           [parser-fn true]
           (and (dtype-dt/datetime-datatype? parser-datatype)
                (string? parser-fn))
           (datetime-formatter-parser-fn parser-datatype
                                         (DateTimeFormatter/ofPattern parser-fn))
           (and (dtype-dt/datetime-datatype? parser-datatype)
                (instance? DateTimeFormatter parser-fn))
           (datetime-formatter-parser-fn parser-datatype
                                         parser-fn)
           :else
           (errors/throwf "Unrecoginzed parser fn type: %s" (type parser-fn)))]))
    [parser-kwd [(find-fixed-parser parser-kwd) false]]))


(defn make-fixed-parser
  [cname parser-kwd]
  (let [[dtype [parse-fn relaxed?]] (parser-entry->parser-tuple parser-kwd)
        [failed-values failed-indexes] (when relaxed?
                                         [(dtype/make-container :list :object 0)
                                          (bitmap/->bitmap)])
        container (column-base/make-container dtype)
        missing-value (column-base/datatype->missing-value dtype)
        missing (bitmap/->bitmap)]
    (FixedTypeParser. container dtype missing-value parse-fn
                      missing failed-values failed-indexes
                      cname)))


(defn parser-kwd-list->parser-tuples
  [kwd-list]
  (mapv parser-entry->parser-tuple kwd-list))


(def default-parser-datatype-sequence
  [:boolean :int16 :int32 :int64 :float64 :uuid
   :packed-duration :packed-local-date
   :zoned-date-time :string :text])


(defn- promote-container
  ^PrimitiveList [old-container ^RoaringBitmap missing new-dtype]
  (let [n-elems (dtype/ecount old-container)
        container (column-base/make-container
                   new-dtype 0)
        missing-value (column-base/datatype->missing-value new-dtype)
        ;;Ensure we unpack a container if we have to promote it.
        old-container (packing/unpack old-container)]
    (.ensureCapacity container n-elems)
    (dotimes [idx n-elems]
      (if (.contains missing idx)
        (.addObject container missing-value)
        (.addObject container (casting/cast
                               (old-container idx)
                               new-dtype))))
    container))


(deftype PromotionalStringParser [^{:unsynchronized-mutable true
                                    :tag PrimitiveList} container
                                  ^{:unsynchronized-mutable true} container-dtype
                                  ^{:unsynchronized-mutable true} missing-value
                                  ^{:unsynchronized-mutable true} parse-fn
                                  ^RoaringBitmap missing
                                  ;;List of datatype,parser-fn tuples
                                  ^List promotion-list
                                  column-name]
  dtype-proto/PECount
  (ecount [this] (.lsize container))
  PParser
  (add-value! [p idx value]
    (when-not (missing-value? value)
      (let [idx (long idx)]
        (let [value-dtype (dtype/elemwise-datatype value)]
          (cond
            (= value-dtype container-dtype)
            (do
              (add-missing-values! container missing missing-value idx)
              (.add container value))
            parse-fn
            (let [parsed-value (parse-fn value)]
              (cond
                (= parsed-value parse-failure)
                (let [start-idx (argops/index-of container-dtype
                                                 (mapv first promotion-list))
                      n-elems (.size promotion-list)
                      next-idx (if (== start-idx -1)
                                 -1
                                 (long (loop [idx (inc start-idx)]
                                         (if (< idx n-elems)
                                           (let [[_container-datatype parser-fn]
                                                 (.get promotion-list idx)
                                                 parsed-value (parser-fn value)]
                                             (if (= parsed-value parse-failure)
                                               (recur (inc idx))
                                               idx))
                                           -1))))
                      [parser-datatype new-parser-fn] (if (== -1 next-idx)
                                                        [:object nil]
                                                        (.get promotion-list next-idx))
                      parsed-value (if new-parser-fn
                                     (new-parser-fn value)
                                     value)
                      new-container (promote-container container missing
                                                       parser-datatype)
                      new-missing-value (column-base/datatype->missing-value
                                         parser-datatype)]
                  (set! container new-container)
                  (set! container-dtype parser-datatype)
                  (set! missing-value new-missing-value)
                  (set! parse-fn new-parser-fn)
                  (add-missing-values! new-container missing new-missing-value idx)
                  (.add new-container parsed-value))
                (not-missing? parsed-value missing-value)
                (do
                  (add-missing-values! container missing missing-value idx)
                  (.addObject container parsed-value))))
            :else
            (do
              (add-missing-values! container missing missing-value idx)
              (.addObject container value)))))))
  (finalize! [p rowcount]
    (finalize-parser-data! container missing nil nil
                           missing-value rowcount)))


(defn promotional-string-parser
  ([column-name parser-datatype-sequence]
   (let [first-dtype (first parser-datatype-sequence)]
     (PromotionalStringParser. (column-base/make-container first-dtype)
                               first-dtype
                               false
                               (default-coercers first-dtype)
                               (bitmap/->bitmap)
                               (mapv (juxt identity default-coercers)
                                     parser-datatype-sequence)
                               column-name)))
  ([column-name]
   (promotional-string-parser column-name default-parser-datatype-sequence)))


(deftype PromotionalObjectParser [^{:unsynchronized-mutable true
                                    :tag PrimitiveList} container
                                  ^{:unsynchronized-mutable true} container-dtype
                                  ^{:unsynchronized-mutable true} missing-value
                                  ^RoaringBitmap missing
                                  column-name]
  dtype-proto/PECount
  (ecount [this] (.lsize container))
  PParser
  (add-value! [p idx value]
    (when-not (missing-value? value)
      (let [idx (long idx)
            arg-type (arg-type value)
            org-datatype (if (= :scalar arg-type)
                           (dtype/elemwise-datatype value)
                           :object)
            packed-dtype (packing/pack-datatype org-datatype)
            container-ecount (dtype/ecount container)]
        (if (or (== 0 container-ecount)
                (= container-dtype packed-dtype))
          (let [value (packing/pack value)]
            (when (== 0 container-ecount)
              (set! container (column-base/make-container packed-dtype))
              (set! container-dtype packed-dtype)
              (set! missing-value (column-base/datatype->missing-value packed-dtype)))
            (when-not (== container-ecount idx)
              (add-missing-values! container missing missing-value idx))
            (.addObject container value))
          (let [widest-datatype (casting/widest-datatype container-dtype packed-dtype)]
            (when-not (= widest-datatype container-dtype)
              (let [new-container (promote-container container
                                                     missing widest-datatype)]
                (set! container new-container)
                (set! container-dtype widest-datatype)
                (set! missing-value (column-base/datatype->missing-value
                                     widest-datatype))))
            (when-not (== container-ecount idx)
              (add-missing-values! container missing missing-value idx))
            (.addObject container value))))))
  (finalize! [p rowcount]
    (finalize-parser-data! container missing nil nil
                           missing-value rowcount)))


(defn promotional-object-parser
  [column-name]
  (PromotionalObjectParser. (dtype/make-container :list :boolean 0)
                            :boolean
                            false
                            (bitmap/->bitmap)
                            column-name))
