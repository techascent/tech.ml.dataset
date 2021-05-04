(ns tech.v3.dataset.io.column-parsers
  "Per-column parsers."
  (:require [tech.v3.dataset.io.datetime :as parse-dt]
            [tech.v3.dataset.impl.column-base :as column-base]
            [tech.v3.datatype.packing :as packing]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.bitmap :as bitmap]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.argops :as argops]
            [tech.v3.datatype.datetime :as dtype-dt]
            [tech.v3.datatype.protocols :as dtype-proto])
  (:import [java.util UUID List HashMap]
           [java.util.function Function Consumer]
           [tech.v3.datatype PrimitiveList]
           [tech.v3.dataset Text]
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


(def parse-failure :tech.v3.dataset/parse-failure)
(def missing :tech.v3.dataset/missing)


(defn make-safe-parse-fn
  [parser-fn]
  (fn [str-val]
    (try
      (parser-fn str-val)
      (catch Throwable e
        parse-failure))))


(def default-coercers
  (merge
   {:bool #(if (string? %)
             (let [^String data %]
               (cond
                 (.equals "true" data) true
                 (.equals "false" data) false
                 :else parse-failure))
             (boolean %))
    :boolean #(if (string? %)
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
    :text #(Text. (str %))}
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


(definterface PParser
  (addValue [^long idx value])
  (finalize [^long rowcount]))

(defn add-value!
  [^PParser p ^long idx value]
  (.addValue p idx value))


(defn finalize!
  [^PParser p ^long rowcount]
  (.finalize p rowcount))


(defn add-missing-values!
  [^PrimitiveList container ^RoaringBitmap missing
   missing-value ^long idx]
  (let [n-elems (.lsize container)]
    (when (< n-elems idx)
      (loop [n-elems n-elems]
        (when (< n-elems idx)
          (.addObject container missing-value)
          (.add missing n-elems)
          (recur (unchecked-inc n-elems)))))))


(defn finalize-parser-data!
  [container missing failed-values failed-indexes
   missing-value column-statistics* rowcount]
  (add-missing-values! container missing missing-value rowcount)
  (let [unparsed-metadata (when (and failed-values
                                     (not= 0 (dtype/ecount failed-values)))
                            {:unparsed-data failed-values
                             :unparsed-indexes failed-indexes})
        col-stats-meta (when column-statistics*
                         {:statistics @column-statistics*})]
    (merge
     #:tech.v3.dataset{:data (or (dtype/as-array-buffer container)
                                 (dtype/as-native-buffer container)
                                 container)
                       :missing missing
                       :force-datatype? true}
     (when (or unparsed-metadata
               col-stats-meta)
       #:tech.v3.dataset{:metadata
                         (merge unparsed-metadata
                                col-stats-meta)}))))


(defn- missing-value?
  "Is this a missing value coming from a CSV file"
  [value]
  ;;fastpath for numbers
  (if (instance? Number value)
    false
    (or (nil? value)
        (.equals "" value)
        (identical? value :tech.v3.dataset/missing)
        (and (string? value) (.equalsIgnoreCase ^String value "na")))))


(deftype FixedTypeParser [^PrimitiveList container
                          container-dtype
                          missing-value parse-fn
                          ^RoaringBitmap missing
                          ^PrimitiveList failed-values
                          ^RoaringBitmap failed-indexes
                          column-name
                          ^Consumer column-statistics
                          ^:unsynchronized-mutable ^long max-idx]
  dtype-proto/PECount
  (ecount [this] (inc max-idx))
  PParser
  (addValue [this idx value]
    (let [idx (unchecked-long idx)]
      (set! max-idx (max idx max-idx))
      ;;First pass is to potentially parse the value.  It could already
      ;;be in the space of the container or it could require the parse-fn
      ;;to make it.
      (let [parsed-value (cond
                           (missing-value? value)
                           :tech.v3.dataset/missing
                           (and (identical? (dtype/datatype value) container-dtype)
                                (not (instance? String value)))
                           value
                           :else
                           (parse-fn value))]
        (cond
          ;;ignore it; we will add missing when we see the first valid value
          (identical? :tech.v3.dataset/missing parsed-value)
          nil
          ;;Record the original incoming value if we are parsing in relaxed mode.
          (identical? :tech.v3.dataset/parse-failure parsed-value)
          (if failed-values
            (do
              (.addObject failed-values value)
              (.add failed-indexes (unchecked-int idx)))
            (errors/throwf "Failed to parse value %s as datatype %s on row %d"
                           value container-dtype idx))
          :else
          (do
            (add-missing-values! container missing missing-value idx)
            (.accept column-statistics parsed-value)
            (.addObject container parsed-value))))))
  (finalize [p rowcount]
    (finalize-parser-data! container missing failed-values failed-indexes
                           missing-value column-statistics rowcount)))


(defn- find-fixed-parser
  [kwd]
  (if (= kwd :string)
    str
    (if-let [retval (get default-coercers kwd)]
      retval
      (errors/throwf "Failed to find parser for keyword %s" kwd))))


(defn- datetime-formatter-parser-fn
  [parser-datatype formatter]
  (let [unpacked-datatype (packing/unpack-datatype parser-datatype)
        parser-fn (parse-dt/datetime-formatter-parse-str-fn
                   unpacked-datatype formatter)]
    [(make-safe-parse-fn parser-fn) false]))


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
           (datetime-formatter-parser-fn parser-datatype parser-fn)
           (= :text parser-datatype)
           [(find-fixed-parser parser-datatype)]
           :else
           (errors/throwf "Unrecoginzed parser fn type: %s" (type parser-fn)))]))
    [parser-kwd [(find-fixed-parser parser-kwd) false]]))


(defn make-fixed-parser
  [cname parser-kwd options]
  (let [[dtype [parse-fn relaxed?]] (parser-entry->parser-tuple parser-kwd)
        [failed-values failed-indexes] (when relaxed?
                                         [(dtype/make-container :list :object 0)
                                          (bitmap/->bitmap)])
        container (column-base/make-container dtype options)
        missing-value (column-base/datatype->missing-value dtype)
        missing (bitmap/->bitmap)
        colstats (when (:column-statistics? options)
                   (column-base/column-statistics-consumer dtype options))]
    (FixedTypeParser. container dtype missing-value parse-fn
                      missing failed-values failed-indexes
                      cname colstats -1)))


(defn parser-kwd-list->parser-tuples
  [kwd-list]
  (mapv parser-entry->parser-tuple kwd-list))


(def default-parser-datatype-sequence
  [:bool :int16 :int32 :int64 :float64 :uuid
   :packed-duration :packed-local-date
   :zoned-date-time :string :text :boolean])


(defn- promote-container
  ^PrimitiveList [old-container ^RoaringBitmap missing new-dtype options]
  (let [n-elems (dtype/ecount old-container)
        container (column-base/make-container new-dtype options)
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


(defn- promote-column-statistics
  [old-dtype dtype old-col-statistics]
  (when (or (casting/numeric-type? dtype)
            (dtype-dt/datetime-datatype? dtype))
    (let [new-col-stats (column-base/column-statistics-consumer dtype)]
      (if (or (nil? old-col-statistics)
              (== 0 (dtype/ecount old-col-statistics)))
        new-col-stats
        (try
          (let [{:keys [min-value max-value order]} @old-col-statistics
                old-dtype (dtype/datatype min-value)
                new-dtype (packing/unpack-datatype dtype)
                min-value (dtype/cast min-value dtype)
                max-value (dtype/cast max-value dtype)]
            (when-not (identical? :object
                                  (casting/simple-operation-space
                                   old-dtype new-dtype))
              (case order
                :tech.v3.dataset/== (.accept new-col-stats min-value)
                :tech.v3.dataset/> (do (.accept new-col-stats min-value)
                                       (.accept new-col-stats max-value))
                :tech.v3.dataset/< (do (.accept new-col-stats max-value)
                                       (.accept new-col-stats min-value))
                :tech.v3.dataset/unordered (do (.accept new-col-stats min-value)
                                               (.accept new-col-stats max-value)
                                               (.accept new-col-stats min-value)))
              new-col-stats))
          (catch Exception e
            nil))))))


(defn- find-next-parser
  ^long [value container-dtype ^List promotion-list]
  (let [start-idx (argops/index-of (mapv first promotion-list) container-dtype)
        n-elems (.size promotion-list)]
    (if (== start-idx -1)
      -1
      (long (loop [idx (inc start-idx)]
              (if (< idx n-elems)
                (let [[_container-datatype parser-fn]
                      (.get promotion-list idx)
                      parsed-value (parser-fn value)]
                  (if (= parsed-value parse-failure)
                    (recur (inc idx))
                    idx))
                -1))))))


(defn- resolve-parser-index
  "Resolve the next parser index returning a tuple of [parser-datatype new-parser-fn]"
  [next-idx ^List container container-dtype missing ^List promotion-list]
  (let [next-idx (long next-idx)]
    (if (== -1 next-idx)
      [:object nil]
      (let [n-missing (dtype/ecount missing)
            n-valid (- (dtype/ecount container) n-missing)
            parser-data (.get promotion-list next-idx)
            parser-datatype (first parser-data)]
        ;;Figure out if our promotion process will result in a valid container.
        (cond
          (== 0 n-valid)
          parser-data
          (and (or (identical? :bool container-dtype)
                   (identical? :boolean container-dtype)
                   (casting/numeric-type? container-dtype))
               (casting/numeric-type? (packing/unpack-datatype parser-datatype)))
          parser-data
          :else
          [:string (default-coercers :string)])))))


(deftype PromotionalStringParser [^{:unsynchronized-mutable true
                                    :tag PrimitiveList} container
                                  ^{:unsynchronized-mutable true} container-dtype
                                  ^{:unsynchronized-mutable true} missing-value
                                  ^{:unsynchronized-mutable true} parse-fn
                                  ^RoaringBitmap missing
                                  ;;List of datatype,parser-fn tuples
                                  ^List promotion-list
                                  column-name
                                  ^:unsynchronized-mutable ^long max-idx
                                  ^{:unsynchronized-mutable true
                                    :tag Consumer} column-statistics
                                  options]
  dtype-proto/PECount
  (ecount [this] (inc max-idx))
  PParser
  (addValue [p idx value]
    (set! max-idx (max idx max-idx))
    (let [parsed-value
          (cond
            (missing-value? value)
            :tech.v3.dataset/missing

            (identical? (dtype/datatype value) container-dtype)
            value

            ;;If we have a function to parse the data
            parse-fn
            (let [parsed-value (parse-fn value)]
              ;;If the value parsed successfully
              (if (not (identical? :tech.v3.dataset/parse-failure parsed-value))
                parsed-value
                ;;else Perform column promotion
                (let [next-idx (find-next-parser value container-dtype promotion-list)
                      [parser-datatype new-parser-fn]
                      (resolve-parser-index next-idx container container-dtype
                                            missing promotion-list)
                      parsed-value (if new-parser-fn
                                     (new-parser-fn value)
                                     value)
                      new-container (promote-container container missing
                                                       parser-datatype
                                                       options)
                      new-col-stats (promote-column-statistics
                                     container-dtype parser-datatype column-statistics)
                      new-missing-value (column-base/datatype->missing-value
                                         parser-datatype)]
                  ;;Update member variables based on new parser
                  (set! container new-container)
                  (set! container-dtype parser-datatype)
                  (set! missing-value new-missing-value)
                  (set! parse-fn new-parser-fn)
                  (set! column-statistics new-col-stats)
                  parsed-value)))
            ;;Else, nothing to parse with, just return string value
            :else
            value)]
      (cond
        ;;Promotional parsers should not have parse failures.
        (identical? :tech.v3.dataset/parse-failure parsed-value)
        (errors/throwf
         "Parse failure detected in promotional parser - Please file issue.")
        (identical? :tech.v3.dataset/missing parsed-value)
        nil ;;Skip, will add missing on next valid value
        :else
        (do
          (add-missing-values! container missing missing-value idx)

          (.addObject container parsed-value)
          (when column-statistics
            (.accept column-statistics parsed-value))))))
  (finalize [p rowcount]
    (finalize-parser-data! container missing nil nil missing-value
                           column-statistics rowcount)))


(defn promotional-string-parser
  ([column-name parser-datatype-sequence options]
   (let [first-dtype (first parser-datatype-sequence)]
     (PromotionalStringParser. (column-base/make-container
                                (if (= :bool first-dtype)
                                  :boolean
                                  first-dtype)
                                options)
                               first-dtype
                               false
                               (default-coercers first-dtype)
                               (bitmap/->bitmap)
                               (mapv (juxt identity default-coercers)
                                     parser-datatype-sequence)
                               column-name
                               -1
                               nil
                               options)))
  ([column-name options]
   (promotional-string-parser column-name default-parser-datatype-sequence options)))


(deftype PromotionalObjectParser [^{:unsynchronized-mutable true
                                    :tag PrimitiveList} container
                                  ^{:unsynchronized-mutable true} container-dtype
                                  ^{:unsynchronized-mutable true} missing-value
                                  ^RoaringBitmap missing
                                  column-name
                                  ^:unsynchronized-mutable ^long max-idx
                                  ^{:unsynchronized-mutable true
                                    :tag Consumer} column-statistics
                                  options]
  dtype-proto/PECount
  (ecount [this] (inc max-idx))
  PParser
  (addValue [p idx value]
    (set! max-idx (max idx max-idx))
    (when-not (missing-value? value)
      (let [org-datatype (dtype/datatype value)
            packed-dtype (packing/pack-datatype org-datatype)
            container-ecount (.lsize container)]
        (if (or (== 0 container-ecount)
                (= container-dtype packed-dtype))
          (do
            (when (== 0 container-ecount)
              (set! container (column-base/make-container packed-dtype options))
              (set! container-dtype packed-dtype)
              (set! column-statistics (column-base/column-statistics-consumer
                                       org-datatype))
              (set! missing-value (column-base/datatype->missing-value packed-dtype)))
            (when-not (== container-ecount idx)
              (add-missing-values! container missing missing-value idx))
            (.addObject container value)
            (when column-statistics
              (.accept column-statistics value)))
          (let [widest-datatype (casting/widest-datatype
                                 (packing/unpack-datatype container-dtype)
                                 org-datatype)]
            (when-not (= widest-datatype container-dtype)
              (let [new-container (promote-container container
                                                     missing widest-datatype
                                                     options)
                    new-stats (promote-column-statistics
                               container-dtype widest-datatype column-statistics)]
                (set! container new-container)
                (set! container-dtype widest-datatype)
                (set! missing-value (column-base/datatype->missing-value
                                     widest-datatype))
                (set! column-statistics new-stats)))
            (when-not (== container-ecount idx)
              (add-missing-values! container missing missing-value idx))
            (.addObject container value)
            (when column-statistics
              (.accept column-statistics value)))))))
  (finalize [p rowcount]
    (finalize-parser-data! container missing nil nil
                           missing-value column-statistics rowcount)))


(defn promotional-object-parser
  [column-name options]
  (PromotionalObjectParser. (dtype/make-container :list :boolean 0)
                            :boolean
                            false
                            (bitmap/->bitmap)
                            column-name
                            -1
                            nil
                            options))
