(ns tech.v3.dataset.big-text
  (:require [tech.v3.datatype :as dtype]
            [clojure.java.io :as io]
            [tech.v3.dataset :as ds]
            [tech.v3.datatype.mmap :as mmap]
            [pppmap.core :as ppp]
            )
  (:import [java.nio.charset Charset]
           [tech.v3.datatype ObjectReader ]

           )
  )

(defn reset-file [fpath]
 (with-open [o (io/output-stream fpath)]))

(defn str->mmap [str mmap-file]
  (let [file (io/file mmap-file)
        file-length (.length file)
        bytes (.getBytes str (Charset/forName "UTF-8"))]
    (with-open [o (io/output-stream file :append true)]
      (.write o bytes))
    {:offset file-length :length (count bytes)
     :mmap (mmap/mmap-file mmap-file) }))

   (defn extract-string [mmap offset length]
     (String.
      (dtype/->byte-array
       (dtype/sub-buffer mmap offset length))))


(defn add-mmap-text-column [ds pointer-col-name text-col-name]
  (let [offset-length->text-reader
        (reify ObjectReader
          (elemwiseDatatype [rdr] :object)
          (lsize [rdr] (ds/row-count ds) )
          (readObject [rdr _idx]
            (let [pointer (nth (get ds pointer-col-name ) _idx)
                  mmap (:mmap pointer)
                  offset (:offset pointer)
                  length (:length pointer)]
              (if (nil? offset)
                nil
                (extract-string mmap offset length)))))]
       (ds/add-or-update-column ds (ds/new-column text-col-name offset-length->text-reader {} []))))



(comment



  (def mmap-file-name "/tmp/abstract-column.bin")


  (reset-file mmap-file-name)
  (def ds
    (-> "./test/data/medical-text.csv"
        (ds/->dataset {:key-fn keyword
                       :column-whitelist ["cord_uid" "abstract"]
                       :max-chars-per-column 10000000
                       :parser-fn {:abstract [:object #(str->mmap % mmap-file-name )]} })
        (ds/rename-columns {:abstract :abstract-pointer})
        (add-mmap-text-column  :abstract-pointer :abstract)))







  (time
   (tech.v3.datatype.reductions/double-summation
    (dtype/emap #(double  (count %))
                :double
                (ds :abstract))))
  (time
   (reduce +
           (dtype/emap count
                       :long
                       (ds :abstract))))

  )


(comment
  (def mmap-file-name "/tmp/abstract-column.bin")

  (reset-file mmap-file-name)

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
              (ds/->dataset {:text-pointers [text]}
                            {
                             :parser-fn {:text-pointers [:object #(str->mmap % mmap-file-name )]}})
              (add-mmap-text-column  :text-pointers :text)

              )
          ]
      ds
      ))

  (def all
    (apply ds/concat
           (map file->ds files)))


  (ds/row-count all)

  (def x
    (time
     (ds/new-column :test
                    (doall
                     (map #(count
                            (if (nil? %)
                              []
                              (clojure.string/split % #"\\w")))
                          (:text all)))
                    )))
  (def x
    (time
     (ds/new-column :test
                    (doall
                     (pmap #(count
                            (if (nil? %)
                              []
                              (clojure.string/split % #"\\w")))
                          (:text all)))
                    )))
  (def x
    (time
     (ds/new-column :test
                    (doall
                     (ppp/ppmap-with-progress "tokenize" 50 #(count
                            (if (nil? %)
                              []
                              (clojure.string/split % #"\\w")))
                          (:text all)))
                    )))

  )
