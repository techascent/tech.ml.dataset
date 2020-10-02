(ns ^:no-doc tech.v3.dataset.io.datetime
  (:require [clojure.string :as s]
            [tech.v3.datatype.datetime :as dtype-dt])
  (:import [java.time LocalDate LocalDateTime LocalTime
            ZonedDateTime OffsetDateTime Instant Duration]
           [java.time.format DateTimeFormatter DateTimeFormatterBuilder]))


(set! *warn-on-reflection* true)


;;Assumping is that the string will have /,-. replaced with /space
(def date-parser-patterns
  ["yyyy MM dd"
   "yyyyMMdd"
   "MM dd yyyy"
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
      (let [duration-str (.trim str-data)
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


(def datatype->general-parse-fn-map
  {:local-date parse-local-date
   :local-date-time parse-local-date-time
   :duration parse-duration
   :local-time parse-local-time
   :instant #(Instant/parse ^String %)
   :zoned-date-time #(ZonedDateTime/parse ^String %)})


(defn datetime-formatter-parse-str-fn
  [datatype formatter]
  (case datatype
    :local-date
    #(LocalDate/parse % formatter)
    :local-date-time
    #(LocalDateTime/parse % formatter)
    :local-time
    #(LocalTime/parse % formatter)
    :zoned-date-time
    #(ZonedDateTime/parse % formatter)
    :offset-date-time
    #(OffsetDateTime/parse % formatter)))


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
