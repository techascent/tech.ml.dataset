(ns tech.v3.dataset.mmap-string-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is]]
            [tech.v3.dataset :as ds])
  (:import java.nio.channels.FileChannel
           java.nio.file.StandardOpenOption))

(deftest parsing-as-mmap-string-works
  (let [ds
        (-> "./test/data/medical-text.csv"
            (ds/->dataset {:key-fn keyword
                           :column-whitelist ["cord_uid" "abstract"]
                           :max-chars-per-column 10000000
                           :parser-fn {:abstract :mmap-string} })

            )]
    (is (str/starts-with? (last (:abstract ds)) "Pneumonia"))
    (is (str/starts-with? (first (:abstract ds)) "OBJECTIVE"))
    (is (= 99 (ds/row-count ds)))))

(deftest can-use-column-options
  (let [file (java.io.File/createTempFile "tmd" ".mmap")
        channel
        (FileChannel/open (.toPath file)
                          (into-array [StandardOpenOption/APPEND]))
        ds
        (-> "./test/data/medical-text.csv"
            (ds/->dataset {:key-fn keyword
                           :column-whitelist ["cord_uid" "abstract"]
                           :max-chars-per-column 10000000
                           :column-opts {:abstract {:mmap-file file
                                                    :mmap-file-channel channel}}

                           :parser-fn {:abstract :mmap-string}})

            )]
    (is (= (.position channel) 124866))
    (is (= (.length file) 124866))))
