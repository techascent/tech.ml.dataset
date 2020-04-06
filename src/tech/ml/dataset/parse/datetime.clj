(ns tech.ml.dataset.parse.datetime
  (:require [clojure.string :as s])
  (:import [java.time LocalDate LocalDateTime LocalTime
            ZonedDateTime]
           [java.time.format DateTimeFormatter
            DateTimeFormatterBuilder]))


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
  (.replaceAll data "[.]" ":"))


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
