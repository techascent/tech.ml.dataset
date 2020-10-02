(ns tech.v3.dataset.io.nippy
  (:require [tech.v3.dataset.io :as ds-io]
            [tech.v3.dataset.base :as ds-base]
            [tech.v3.datatype.errors :as errors]
            [tech.io :as io]
            [taoensso.nippy :as nippy])
  (:import [tech.v3.dataset.impl.dataset Dataset]))


(nippy/extend-freeze
 Dataset :tech.ml/dataset
 [ds out]
 (nippy/-freeze-without-meta! (ds-base/dataset->data ds) out))


(nippy/extend-thaw
 :tech.ml/dataset
 [in]
 (-> (nippy/thaw-from-in! in)
     (ds-base/data->dataset)))


(defmethod ds-io/data->dataset :nippy
  [data options]
  (ds-io/wrap-stream-fn
   data (:gzipped? options)
   (fn [instream]
     (let [retval (io/get-nippy instream)]
       (errors/when-not-errorf
        (instance? Dataset retval)
        "Unthawed data is not a dataset: %s"
        (type retval))
       retval))))


(defmethod ds-io/dataset->data! :nippy
  [dataset output options]
  (io/put-nippy! output dataset))
