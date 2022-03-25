(ns tech.v3.dataset.io.dtype-csv
  (:require [tech.v3.datatype.char-input :as char-input]
            [tech.v3.dataset.io :as ds-io]
            [tech.v3.parallel.for :as pfor]
            [tech.v3.parallel.queue-iter :as queue-iter]
            [tech.v3.datatype :as dtype]
            [tech.v3.io :as io]
            [tech.v3.dataset.io.column-parsers :as column-parsers]
            [tech.v3.dataset.io.context :as parse-context])
  (:import [tech.v3.datatype ArrayHelpers]
           [java.lang AutoCloseable]
           [java.util Iterator]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defn- parse-next-batch
  [^Iterator row-iter header-row options]
  (when (.hasNext row-iter)
    (let [n-header-cols (count header-row)
          num-rows (long (get options :batch-size
                              (get options :num-rows Long/MAX_VALUE)))
          {:keys [parsers col-idx->parser]}
          (parse-context/options->col-idx-parse-context
           options :string (fn [^long col-idx]
                             (when (< col-idx n-header-cols)
                               (header-row col-idx))))]
      (loop [row-idx 0
             continue? (.hasNext row-iter)]
        (when (and continue? (< row-idx num-rows))
          (let [row (.next row-iter)]
            (pfor/indexed-doiter
             col-idx data row
             (let [parser (col-idx->parser col-idx)]
               (column-parsers/add-value! parser row-idx data))))
          (recur (unchecked-inc row-idx) (.hasNext row-iter))))
      (cons (parse-context/parsers->dataset options parsers)
            (lazy-seq (parse-next-batch row-iter header-row options))))))


(defn- rows->dataset-seq
  "Given a sequence of rows each row container a sequence of strings, parse into columnar data.
  See csv->columns."
  [{:keys [header-row?]
    :or {header-row? true}
    :as options}
   row-seq]
  (let [row-iter (pfor/->iterator row-seq)
        header-row (if (and header-row? (.hasNext row-iter))
                     (vec (.next row-iter))
                     [])]
    (parse-next-batch row-iter header-row options)))


(defn csv->dataset-seq
  "Read a csv into a dataset."
  [input & [options]]
  (->> (char-input/read-csv (io/input-stream input) options)
       (rows->dataset-seq options)))


(defn csv->dataset
  "Read a csv into a dataset.  Input must be an input-stream.  If it is a string, it will
  be interpreted as the csv data itself."
  [input & [options]]
  (let [options (assoc options :batch-size Long/MAX_VALUE)
        iter (char-input/read-csv (io/input-stream input) options)
        retval (->> (rows->dataset-seq options iter)
                    (first))]
    (when (instance? AutoCloseable iter)
      (.close ^AutoCloseable iter))
    retval))


(defn- load-csv
  [data options]
  (ds-io/wrap-stream-fn
   data (:gzipped? options)
   #(csv->dataset %1 options)))


(defmethod ds-io/data->dataset :csv
  [data options]
  (load-csv data options))


(defmethod ds-io/data->dataset :tsv
  [data options]
  (load-csv data (merge {:separator \tab} options)))


(defmethod ds-io/data->dataset :txt
  [data options]
  (load-csv data options))


(comment
  (require '[tech.v3.dataset.io.univocity :as univocity])
  (require '[criterium.core :as crit])


  (crit/quick-bench (univocity/csv->dataset "test/data/issue-292.csv"))
  ;; Evaluation count : 24 in 6 samples of 4 calls.
  ;;            Execution time mean : 27.045594 ms
  ;;   Execution time std-deviation : 887.643388 µs
  ;;  Execution time lower quantile : 26.015627 ms ( 2.5%)
  ;;  Execution time upper quantile : 27.984189 ms (97.5%)
  ;;                  Overhead used : 1.721587 ns
  (crit/quick-bench (csv->dataset "test/data/issue-292.csv"))
  ;; Evaluation count : 6 in 6 samples of 1 calls.
  ;;            Execution time mean : 139.203976 ms
  ;;   Execution time std-deviation : 3.406500 ms
  ;;  Execution time lower quantile : 136.348543 ms ( 2.5%)
  ;;  Execution time upper quantile : 143.004906 ms (97.5%)
  ;;                  Overhead used : 1.721587 ns

  (crit/quick-bench (ds-csv->dataset "test/data/issue-292.csv"))

  (dotimes [idx 1000]
    (ds-csv->dataset "test/data/issue-292.csv"))


  (with-bindings {#'*compile-path* "compiled"}
    (compile 'tech.v3.dataset.io.csv))

  (defn read-all
    [^java.io.Reader reader]
    (loop [data (.read reader)]
      (if (== -1 data)
        :ok
        (recur (.read reader)))))


  (defn read-all-cbuf
    [^java.io.Reader reader]
    (let [cbuf (char-array 1024)]
      (loop [data (.read reader)]
        (if (== -1 data)
          :ok
          (recur (.read reader cbuf))))))


  (defn read-all-cbuf-ibuf
    [^java.io.Reader reader]
    (let [cbuf (char-array 1024)
          ibuf (int-array 1024)]
      (loop [data (.read reader cbuf)]
        (if (== -1 data)
          :ok
          (do
            (dotimes [idx data]
              (ArrayHelpers/aset ibuf idx (clojure.lang.RT/intCast (aget cbuf idx))))
            (recur (.read reader cbuf)))))))


  )
