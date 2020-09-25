(ns tech.libs.arrow
  (:require [tech.parallel.utils :refer [export-symbols]]
            [tech.v2.datatype.mmap :as mmap]))


(export-symbols tech.libs.arrow.copying
                write-dataset-to-stream!
                write-dataset-seq-to-stream!
                stream->dataset-seq-copying
                read-stream-dataset-copying)


(export-symbols tech.libs.arrow.in-place
                message-seq
                parse-message
                read-stream-dataset-inplace
                stream->dataset-seq-inplace)


(defn visualize-arrow-stream
  "Loads an arrow file via mmap pathway and parses the file into a lower-level
  description that prints well to the REPL.  Useful for quickly seeing what is in
  an Arrow stream.  Returned value can be used to construct datasets via
  in-place/parse-next-dataset.
  See source code to stream->dataset-seq-inplace.
  The default resrouce-type of the file is :gc as this is intended to be used
  for explorational purposes."
  [fname & [options]]
  (let [file-data (mmap/mmap-file fname (merge {:resource-type :gc} options))]
    {:file-data file-data
     :arrow-data (map parse-message (message-seq file-data))}))
