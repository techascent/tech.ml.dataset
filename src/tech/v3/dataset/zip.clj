(ns tech.v3.dataset.zip
  (:require [tech.v3.dataset.io :as ds-io]
            [tech.v3.io :as io]
            [clojure.tools.logging :as log])
  (:import [java.util.zip ZipInputStream ZipOutputStream ZipEntry]
           [tech.v3.dataset NoCloseInputStream]))


(set! *warn-on-reflection* true)


(defmethod ds-io/data->dataset :zip
  [data options]
  (with-open [is (-> (apply io/input-stream data (apply concat (seq options)))
                     (ZipInputStream.))]
    (let [zentry (.getNextEntry is)
          ftype (-> (ds-io/str->file-info (.getName zentry))
                    (get :file-type))
          retval (ds-io/data->dataset (NoCloseInputStream. is)
                                      (assoc options :file-type ftype))]
      (when (.getNextEntry is)
        (log/warnf "Multiple entries found in zipfile"))
      retval)))


(defmethod ds-io/dataset->data! :zip
  [data output options]
  (let [inner-name (.substring (str output) 0 (- (count output) 4))
        ftype (-> (ds-io/str->file-info inner-name)
                  (get :file-type))]
    (with-open [os (-> (apply io/output-stream! output (apply concat (seq options)))
                       (ZipOutputStream.))]
      (.putNextEntry os (ZipEntry. inner-name))
      (ds-io/dataset->data! data os (assoc options :file-type ftype)))))
