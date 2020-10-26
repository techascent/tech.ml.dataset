(ns tech.v3.dataset.main
  (:require [tech.v3.dataset :as ds]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.libs.arrow :as arrow]
            [clojure.tools.logging :as log])
  (:gen-class))


(defn -main
  [& args]
  ;;Load larray.  Note this implies a pre-build step -- unpack-larray
  (let [libname (format "%s/resources/%s"
                       (System/getProperty "user.dir")
                       (System/mapLibraryName "larray"))]
    (log/infof "Loading shared library: %s" libname)
    (System/load libname))
  (let [test-ds (ds/->dataset "test/data/stocks.csv")]
    (println test-ds)
    (println (dfn/mean (test-ds "price")))
    (arrow/write-dataset-to-stream! test-ds "stocks.arrow")
    (println (dfn/mean ((arrow/read-stream-dataset-inplace "stocks.arrow") "price")))
    0))
