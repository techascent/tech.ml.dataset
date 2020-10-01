(ns tech.v3.dataset.parse
  (:require [tech.io :as io])
  (:import [java.io InputStream]))


(defn str->file-info
  [^String file-str]
  (let [file-str (.toLowerCase ^String file-str)
        gzipped? (.endsWith file-str ".gz")
        file-str (if gzipped?
                   (.substring file-str 0 (- (count file-str) 3))
                   file-str)
        last-period (.lastIndexOf file-str ".")
        file-type (if-not (== -1 last-period)
                    (keyword (.substring file-str (inc last-period)))
                    :unknown)]
    {:gzipped? gzipped?
     :file-type file-type}))


(defn wrap-stream-fn
  [dataset gzipped? open-fn]
  (with-open [^InputStream istream (if (instance? InputStream dataset)
                                     dataset
                                     (if gzipped?
                                       (io/gzip-input-stream dataset)
                                       (io/input-stream dataset)))]
    (open-fn istream)))


(defmulti data->dataset
  (fn [data options]
    (:file-type options)))


(defmethod data->dataset :default
  [data options]
  (throw (format "Unrecognized read file type: %s"
                 (:file-type options))))


(defmulti dataset->data!
  (fn [ds output options]
    (:file-type options)))


(defmethod dataset->data! :default
  [ds output options]
  (throw (format "Unrecognized write file type: %s"
                 (:file-type options))))
