(ns tech.v3.dataset.io.datetime
  "Helpful and well tested string->datetime pathways."
  (:require [clojure.string :as s]
            [tech.v3.datatype.datetime.constants :as dtype-dt])
  (:import [java.time LocalDate LocalDateTime LocalTime
            ZonedDateTime OffsetDateTime Instant Duration]
           [java.time.format DateTimeFormatter DateTimeFormatterBuilder]))


(set! *warn-on-reflection* true)


;;Assumping is that the string will have /,-. replaced with /space
(def ^{:doc "Local date parser patterns used to generate the local date
formatter."} local-date-parser-patterns
  ["yyyy MM dd"
   "yyyyMMdd"
   "MM dd yyyy"
   "dd MMM yyyy"
   "M d yyyy"
   "M d yy"
   "MMM dd yyyy"
   "MMM dd yy"
   "MMM d yyyy"])


(defn- date-preparse
  ^String [^String data]
  (.replaceAll data "[/,-. ]+" " "))


(def ^{:doc "DateTimeFormatter that runs through a set of options in order to
parse a wide variety of local date formats."
       :tag DateTimeFormatter}
  local-date-formatter
  (let [builder (DateTimeFormatterBuilder.)]
    (.parseCaseInsensitive builder)
    (doseq [pattern local-date-parser-patterns]
      (.appendOptional builder (DateTimeFormatter/ofPattern pattern)))
    (.appendOptional builder DateTimeFormatter/ISO_LOCAL_DATE)
    (.toFormatter builder)))


(defn parse-local-date
  "Convert a string into a local date attempting a wide variety of format types."
  ^LocalDate [^String str-data]
  (LocalDate/parse (date-preparse str-data) local-date-formatter))


(def ^{:doc "Parser patterns to parse a wide variety of time strings"}
  time-parser-patterns
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


(def ^{:doc "DateTimeFormatter built to help parse a wide variety of time strings"
       :tag DateTimeFormatter}
  local-time-formatter
  (let [builder (DateTimeFormatterBuilder.)]
    (.parseCaseInsensitive builder)
    (doseq [pattern time-parser-patterns]
      (.appendOptional builder (DateTimeFormatter/ofPattern pattern)))
    (.appendOptional builder DateTimeFormatter/ISO_LOCAL_TIME)
    (.toFormatter builder)))


(defn- local-time-preparse
  ^String [^String data]
  (.replaceAll data "[._]" ":"))


(defn parse-local-time
  "Convert a string into a local time attempting a wide variety of
  possible parser patterns."
  ^LocalTime [^String str-data]
  (LocalTime/parse (local-time-preparse str-data) local-time-formatter))


(defn parse-local-date-time
  "Parse a local-date-time by first splitting the string and then separately
  parsing the local-date portion and the local-time portions."
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
  "Attempt a few ways to parse a duration."
  ^Duration [^String str-data]
  (try
    (Duration/parse str-data)
    (catch Throwable _e
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


(def ^{:doc "Map of datetime datatype to generalized parse fn."}
  datatype->general-parse-fn-map
  {:local-date parse-local-date
   :local-date-time parse-local-date-time
   :duration parse-duration
   :local-time parse-local-time
   :instant #(Instant/parse ^String %)
   :zoned-date-time #(ZonedDateTime/parse ^String %)})


(defn datetime-formatter-parse-str-fn
  "Given a datatype and a formatter return a function that
  attempts to parse that specific datatype."
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
