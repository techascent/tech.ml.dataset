(ns tech.v3.dataset.bzip2
  "API for reading/writing a single dataset to/from a bzip2 file.  File must
  only contain one entry.

  You must have apache commons-compress included as part of your project to
  load this namespace.  Many projects already include commons-compress so manually
  including it may not be necessary for your system.

  Also note that version before 1.21 have several known vulnerabilities.

```clojure
  org.apache.commons/commons-compress {:mvn/version \"1.21\"}
```

### Usage

```clojure
user> (require '[tech.v3.dataset :as ds])
nil
user> (require '[tech.v3.dataset.bzip2])
nil
user> (def stocks (ds/->dataset \"test/data/stocks.csv\"))
#'user/stocks
user> (ds/write! stocks \"test/data/stocks.csv.bz2\")
nil
user> (ds/head (ds/->dataset \"test/data/stocks.csv.bz2\"))
test/data/stocks.csv.bz2 [5 3]:

| symbol |       date | price |
|--------|------------|------:|
|   MSFT | 2000-01-01 | 39.81 |
|   MSFT | 2000-02-01 | 36.35 |
|   MSFT | 2000-03-01 | 43.22 |
|   MSFT | 2000-04-01 | 28.37 |
|   MSFT | 2000-05-01 | 25.45 |
```
  "
  (:require [tech.v3.dataset.io :as ds-io]
            [tech.v3.dataset.io.univocity :as univocity]
            [tech.v3.dataset.io.mapseq-colmap :as parse-mapseq-colmap]
            [tech.v3.io :as io])
  (:import [org.apache.commons.compress.compressors.bzip2 BZip2CompressorInputStream
            BZip2CompressorOutputStream]
           [java.io InputStream]))


(set! *warn-on-reflection* true)


(defmethod ds-io/data->dataset :bz2
  [data options]
  (let [ftype (-> (ds-io/str->file-info (.substring (str data) 0
                                                    (- (count data) 4)))
                  (get :file-type))]
    (with-open [is (-> (apply io/input-stream data (apply concat (seq options)))
                       (BZip2CompressorInputStream.))]
      (ds-io/data->dataset is (assoc options :file-type ftype)))))


(defmethod ds-io/dataset->data! :bz2
  [data output options]
  (let [ftype (-> (ds-io/str->file-info (.substring (str output) 0
                                                    (- (count output) 4)))
                  (get :file-type))]
    (with-open [os (-> (apply io/output-stream! output (apply concat (seq options)))
                       (BZip2CompressorOutputStream.))]
      (ds-io/dataset->data! data os (assoc options :file-type ftype)))))
