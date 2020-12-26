(ns tech.v3.dataset.mmap-string-test
  (:require [tech.v3.datatype :as dtype]
            [clojure.java.io :as io]
            [tech.v3.dataset :as ds]
            [tech.v3.datatype.mmap :as mmap]
            [tech.v3.datatype.mmap-string-list :as string-list]
            )
  (:import [java.nio.charset Charset]
           [tech.v3.datatype ObjectReader ]

           )
  )


(comment



  (def ds
    (-> "./test/data/medical-text.csv"
        (ds/->dataset {:key-fn keyword
                       :column-whitelist ["cord_uid" "abstract"]
                       :max-chars-per-column 10000000
                       :parser-fn {:abstract :mmap-string} })

        )))


(comment

  ;; (def files
  ;;   (->> (io/file  "/home/carsten/Dropbox/sources/analyseOpinions/corpus")
  ;;        (file-seq)
  ;;        (filter #(.isFile %))
  ;;        (map #(.getPath %))
  ;;        ))

  ;; (defn file->ds [fpath]
  ;;   (let [text
  ;;         (slurp fpath)
  ;;         ds (->
  ;;             (ds/->dataset {:text-pointers [text]}
  ;;                           {
  ;;                            :parser-fn {:text-pointers [:object #(str->mmap % mmap-file-name )]}})
  ;;             (add-mmap-text-column  :text-pointers :text))]
  ;;     ds
  ;;     ))

  ;; (def all
  ;;   (apply ds/concat
  ;;          (map file->ds files)))

  )
