(ns tech.v3.libs.arrow
  "Support for reading/writing arrow files.  We currently support only the
  arrow 'streaming' file type.  This is indicated via the `ipc` pathways in R
  and is (at the time of writing this documentation) the default for Pandas.


Users must include the apache arrow dependencies in their project:

```clojure
[org.apache.arrow/arrow-memory-unsafe \"2.0.0\"]
[org.apache.arrow/arrow-vector \"2.0.0\" :exclusions [commons-codec]]
```"
  (:require [tech.v3.datatype.export-symbols :refer [export-symbols]]
            [tech.v3.datatype.mmap :as mmap]
            [tech.v3.datatype.native-buffer :as native-buffer]))


(export-symbols tech.v3.libs.arrow.copying
                write-dataset-to-stream!
                write-dataset-seq-to-stream!
                stream->dataset-seq-copying
                read-stream-dataset-copying)


(export-symbols tech.v3.libs.arrow.in-place
                message-seq
                parse-message
                parse-message-printable
                read-stream-dataset-inplace
                stream->dataset-seq-inplace)


(defn visualize-arrow-stream
  "Loads an arrow file via mmap pathway and parses the file into a lower-level
  description that prints well to the REPL.  Useful for quickly seeing what is in
  an Arrow stream.  Returned value can be used to construct datasets via
  in-place/parse-next-dataset.
  See source code to stream->dataset-seq-inplace."
  [fname & [options]]
  (let [file-data (mmap/mmap-file fname (merge {:resource-type :gc} options))]
    {:file-data (native-buffer/native-buffer->map file-data)
     :arrow-data (map parse-message-printable (message-seq file-data))}))
