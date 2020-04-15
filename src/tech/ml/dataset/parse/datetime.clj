(ns tech.ml.dataset.parse.datetime
  (:require [clojure.string :as s]
            [tech.v2.datatype.datetime :as dtype-dt]
            [tech.v2.datatype.casting :as casting]
            [primitive-math :as pmath])
  (:import [java.time LocalDate LocalDateTime LocalTime
            ZonedDateTime OffsetDateTime Instant Duration]
           [tech.v2.datatype.typed_buffer TypedBuffer]
           [java.time.format DateTimeFormatter DateTimeFormatterBuilder]
           [java.util List]
           [it.unimi.dsi.fastutil.ints IntList]
           [it.unimi.dsi.fastutil.longs LongList]))


(set! *warn-on-reflection* true)


;;Assumping is that the string will have /,-. replaced with /space
(def date-parser-patterns
  ["yyyyMMdd"
   "MM dd yyyy"
   "yyyy MM dd"
   "dd MMM yyyy"
   "M d yyyy"
   "M d yy"
   "MMM dd yyyy"
   "MMM dd yy"
   "MMM d yyyy"])


(defn date-preparse
  ^String [^String data]
  (.replaceAll data "[/,-. ]+" " "))


(def base-local-date-formatter
  (let [builder (DateTimeFormatterBuilder.)]
    (.parseCaseInsensitive builder)
    (doseq [pattern date-parser-patterns]
      (.appendOptional builder (DateTimeFormatter/ofPattern pattern)))
    (.appendOptional builder DateTimeFormatter/ISO_LOCAL_DATE)
    (.toFormatter builder)))


(defn parse-local-date
  ^LocalDate [^String str-data]
  (LocalDate/parse (date-preparse str-data)
                   base-local-date-formatter))


(def time-parser-patterns
  ["HH:mm:ss:SSS"
   "hh:mm:ss a"
   "HH:mm:ss"
   "h:mm:ss a"
   "H:mm:ss"
   "hh:mm a"
   "HH:mm"
   "HHmm"
   "h:mm a"
   "H:mm"])


(def base-local-time-formatter
  (let [builder (DateTimeFormatterBuilder.)]
    (.parseCaseInsensitive builder)
    (doseq [pattern time-parser-patterns]
      (.appendOptional builder (DateTimeFormatter/ofPattern pattern)))
    (.appendOptional builder DateTimeFormatter/ISO_LOCAL_TIME)
    (.toFormatter builder)))


(defn local-time-preparse
  ^String [^String data]
  (.replaceAll data "[._]" ":"))


(defn parse-local-time
  ^LocalTime [^String str-data]
  (LocalTime/parse (local-time-preparse str-data)
                   base-local-time-formatter))


(defn parse-local-date-time
  ^LocalDateTime [^String str-data]
  (let [split-data (s/split str-data #"[ T]+")]
    (cond
      (== 2 (count split-data))
      (let [local-date (parse-local-date (first split-data))
            local-time (parse-local-time (second split-data))]
        (LocalDateTime/of local-date local-time))
      (== 3 (count split-data))
      (let [local-date (parse-local-date (first split-data))
            local-time (parse-local-time (str (split-data 1) " " (split-data 2)))]
        (LocalDateTime/of local-date local-time))
      :else
      (throw (Exception. (format "Failed to parse \"%s\" as a LocalDateTime"
                                 str-data))))))


(defn parse-duration
  ^Duration [^String str-data]
  (try
    (Duration/parse str-data)
    (catch Throwable e
      (let [str-data (.trim str-data)
            duration-str (local-time-preparse str-data)
            [duration-str mult] (if (.startsWith duration-str "-")
                                  [(.substring duration-str 1) -1]
                                  [duration-str 1])
            dur-data (s/split duration-str #":")
            _ (when-not (> (count dur-data) 1)
                (throw (Exception. "Not a valid duration: %s")))
            nanos (reduce (fn [nanos [idx next-data]]
                            (+ (long nanos)
                               (long
                                (case (long idx)
                                  0 (* (Integer/parseInt next-data)
                                       (dtype-dt/nanoseconds-in-hour))
                                  1 (* (Integer/parseInt next-data)
                                       (dtype-dt/nanoseconds-in-minute))
                                  2 (* (Integer/parseInt next-data)
                                       (dtype-dt/nanoseconds-in-second))
                                  3 (* (Integer/parseInt next-data)
                                       (dtype-dt/nanoseconds-in-millisecond))))))
                          0
                          (map-indexed vector dur-data))]
        (Duration/ofNanos (* (long mult) (long nanos)))))))


(defn try-parse-datetimes
  "Given unknown string value, attempt to parse out a datetime value.
  Returns tuple of
  [dtype value]"
  [str-value]
  (if-let [date-val (try (parse-local-date str-value)
                         (catch Exception e nil))]
    [:local-date date-val]
    (if-let [time-val (try (parse-duration str-value)
                           (catch Exception e nil))]
      [:duration time-val]
      (if-let [date-time-val (try (parse-local-date-time str-value)
                                  (catch Exception e nil))]
        [:local-date-time date-time-val]
        [:string str-value]))))



(defmacro compile-time-datetime-parse-str
  [datatype str-val]
  (case datatype
    :local-date
    `(parse-local-date ~str-val)
    :local-date-time
    `(parse-local-date-time ~str-val)
    :local-time
    `(parse-local-time ~str-val)
    :packed-local-date
    `(dtype-dt/pack-local-date (parse-local-date ~str-val))
    :packed-local-date-time
    `(dtype-dt/pack-local-date-time (parse-local-date-time ~str-val))
    :packed-local-time
    `(dtype-dt/pack-local-time (parse-local-time ~str-val))
    :duration
    `(parse-duration ~str-val)
    :packed-duration
    `(dtype-dt/pack-duration (parse-duration ~str-val))
    :instant
    `(Instant/parse ~str-val)
    :packed-instant
    `(dtype-dt/pack-instant (Instant/parse ~str-val))
    :zoned-date-time
    `(ZonedDateTime/parse ~str-val)
    :offset-date-time
    `(OffsetDateTime/parse ~str-val)))


(defmacro ^:private make-parse-str-fn
  [datatype str-val]
  `(case ~datatype
     ~@(->> dtype-dt/datetime-datatypes
            (mapcat
             (fn [dtype]
               [dtype
                `(compile-time-datetime-parse-str ~dtype ~str-val)])))))


(defn parse-str
  "Parse a string into a particular datetime type."
  [datatype str-val]
  (make-parse-str-fn datatype str-val))


(defn datetime-formatter-parse-str-fn
  [datatype formatter]
  (case datatype
    :local-date
    #(LocalDate/parse % formatter)
    :local-date-time
    #(LocalDateTime/parse % formatter)
    :local-time
    #(LocalTime/parse % formatter)
    :packed-local-date
    #(dtype-dt/pack-local-date (LocalDate/parse % formatter))
    :packed-local-date-time
    #(dtype-dt/pack-local-date-time (LocalDateTime/parse % formatter))
    :packed-local-time
    #(dtype-dt/pack-local-time (LocalTime/parse % formatter))
    :zoned-date-time
    #(ZonedDateTime/parse % formatter)
    :offset-date-time
    #(OffsetDateTime/parse % formatter)))


(defn as-typed-buffer
  ^TypedBuffer [item] item)

(defn as-list
  ^List [item] item)

(defn as-int-list
  ^IntList [item] item)

(defn as-long-list
  ^LongList [item] item)


(defmacro compile-time-add-to-container!
  [datatype container parsed-val]
  (if (dtype-dt/packed-datatype? datatype)
    (let [datatype (casting/un-alias-datatype datatype)]
      (case datatype
        :int32 `(.add (as-int-list (.backing-store (as-typed-buffer ~container)))
                      (pmath/int ~parsed-val))
        :int64 `(.add (as-long-list (.backing-store (as-typed-buffer ~container)))
                      (pmath/long ~parsed-val))))
    `(.add (as-list ~container) ~parsed-val)))


(defmacro ^:private make-add-to-container-fn
  [datatype container parsed-val]
  `(case ~datatype
     ~@(->> dtype-dt/datetime-datatypes
            (mapcat (fn [dtype]
                      [dtype
                       `(compile-time-add-to-container! ~dtype ~container ~parsed-val)])))))


(defn add-to-container!
  [datatype container parsed-val]
  (make-add-to-container-fn datatype container parsed-val))


(defmacro datetime-can-parse?
  [datatype str-val]
  `(try
     (compile-time-datetime-parse-str ~datatype ~str-val)
     true
     (catch Throwable e#
       false)))

(def all-datetime-datatypes
  (set (concat (flatten (seq dtype-dt/packed-type->unpacked-type-table))
               [:zoned-date-time :offset-date-time])))

(defn datetime-datatype?
  [dtype]
  (boolean (all-datetime-datatypes dtype)))


(defn datetime-formatter-or-str->parser-fn
  "Given a datatype and one of [fn? string? DateTimeFormatter],
  return a function that takes strings and returns datetime objects
  of type datatype."
  [datatype format-string-or-formatter]
  (cond
    (instance? DateTimeFormatter format-string-or-formatter)
    (datetime-formatter-parse-str-fn datatype format-string-or-formatter)
    (string? format-string-or-formatter)
    (datetime-formatter-parse-str-fn
     datatype
     (DateTimeFormatter/ofPattern format-string-or-formatter))
    (fn? format-string-or-formatter)
    format-string-or-formatter
    :else
    (throw (Exception. (format "Unrecognized datetime parser type: %s"
                               format-string-or-formatter)))))
