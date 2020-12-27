(ns tech.v3.dataset.mmap-string-test
  (:require [tech.v3.datatype :as dtype]
            [clojure.java.io :as io]
            [tech.v3.dataset :as ds]
            [tech.v3.datatype.mmap :as mmap]
            [tech.v3.datatype.mmap-string-list :as string-list]
            )
  (:import [java.nio.charset Charset]
           [java.io FileOutputStream]
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

        ))

  (last (:abstract ds))


  (def positions (atom []))
  (def ds
    (-> "./test/data/medical-text.csv"
        (ds/->dataset {:key-fn keyword
                       :column-whitelist ["cord_uid" "abstract"]
                       :max-chars-per-column 10000000
                       :column-opts {:abstract {:mmap-file "/tmp/out.mmap" :positions positions}}
                       :parser-fn {:abstract :mmap-string} })

        ))

  (.positions
   (.data
    (:abstract ds)))

  )


(comment

  (def column-opts
    {:text {:mmap-file "/tmp/corpus.mmap"
            :mmap-file-output-stream (FileOutputStream. "/tmp/corpus.mmap" true)
            }
     })

  (def files
    (->> (io/file  "/home/carsten/Dropbox/sources/analyseOpinions/corpus")
         (file-seq)
         (filter #(.isFile %))
         (map #(.getPath %))
         ))

  (defn file->ds [fpath]
    (let [text
          (slurp fpath)
          ds (->
              (ds/->dataset {:text [text]}
                            { :column-opts column-opts
                             :parser-fn {:text :mmap-string}})
              )]
      ds
      ))


  (def all
    (mapv file->ds files)
    )

  (def one
    (apply ds/concat-inplace all))

  (time
   (reduce +
           (map count (:text one))))

  ;; (-> one :text
  ;;     (#(.data %)) (#(.positions %)))

  )
