(ns ^:no-doc tech.v3.dataset.utils.slf4j-log-level
  (:import [ch.qos.logback.classic Logger]
           [ch.qos.logback.classic Level]))


(defn set-log-level
  "This has to be a reflection call because no one knows what is returned
  by the getLogger fn call.  If you are using logback classic, then a logback
  classic logger is returned.  Else, you are on your own!"
  [log-level]
  (.setLevel
   (org.slf4j.LoggerFactory/getLogger
    (Logger/ROOT_LOGGER_NAME))
   (case log-level
     :all Level/ALL
     :debug Level/DEBUG
     :trace Level/TRACE
     :info Level/INFO
     :warn Level/WARN
     :error Level/ERROR
     :off Level/OFF))
  log-level)
