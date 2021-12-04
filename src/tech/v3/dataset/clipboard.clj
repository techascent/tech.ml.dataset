(ns tech.v3.dataset.clipboard
  "Optional namespace that copies a dataset to the clipboard for pasting into
  applications such as excel or google sheets.

  Reading defaults to 'csv' format while writing defaults to 'tsv' format."
  (:require [tech.v3.dataset :as ds]
            [tech.v3.datatype.errors :as errors])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream]
           [java.awt.datatransfer Clipboard DataFlavor StringSelection]
           [java.nio.charset StandardCharsets]
           [java.awt Toolkit]))


(set! *warn-on-reflection* true)


(defn clipboard
  "Get the system clipboard."
  ^Clipboard []
  (.getSystemClipboard (Toolkit/getDefaultToolkit)))


(defn clipboard->dataset
  "Copy from the clipboard to a dataset.  Options are passed into ->dataset,
  and `:file-type` is defaulted to `:csv`."
  [& [{:keys [file-type]
       :or {file-type :csv}
       :as options}]]
  (try
    (-> (.getTransferData (.getContents (clipboard) nil)
                          (DataFlavor/stringFlavor))
        (str)
        (.getBytes  StandardCharsets/UTF_8)
        (ByteArrayInputStream.)
        (ds/->dataset (assoc options :file-type file-type)))
    (catch Exception e
      (errors/throwf "Failed to copy dataset from the clipboard,
This can happen with headless JDK installations:
%s" e))))


(defn dataset->clipboard
  "Copy a dataset to the clipboard.  File type is defaulted to 'tsv' as this appears
  to have  wider compatibility across various spreadsheet toolkits."
  [x & [{:keys [file-type]
         :or {file-type :tsv}}]]
  (try
    (let [os (ByteArrayOutputStream.)
          _ (ds/write! x os {:file-type file-type})
          selection (StringSelection. (String. (.toByteArray os)
                                               StandardCharsets/UTF_8))]
      (.setContents (clipboard) selection selection))
    (catch Exception e
      (errors/throwf "Failed to copy dataset to the clipboard,
This can happen with headless JDK installations:
%s" e))))
