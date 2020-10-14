(ns tech.v3.dataset.io.string-row-parser
  (:require [tech.v3.dataset.io.column-parsers :as column-parsers]
            [tech.v3.dataset.io.context :as parse-context]
            [tech.v3.datatype :as dtype]
            [tech.v3.parallel.for :as pfor])
  (:import [java.util.function Function]
           [java.util HashMap]))


(set! *warn-on-reflection* true)


(defn rows->dataset
  "Given a sequence of string[] rows, parse into columnar data.
  See csv->columns.
  This method is useful if you have another way of generating sequences of
  string[] row data."
  [{:keys [header-row? skip-bad-rows?]
    :or {header-row? true}
    :as options}
   row-seq]
  (let [initial-row (first row-seq)
        header-row (dtype/->reader (if header-row?
                                     initial-row
                                     []))
        n-header-cols (count header-row)
        row-seq (if header-row?
                  (rest row-seq)
                  row-seq)
        {:keys [parsers col-idx->parser]}
        (parse-context/options->col-idx-parse-context
         options :string (fn [^long col-idx]
                           (when (< col-idx n-header-cols)
                             (header-row col-idx))))]
    ;;side effecting loop
    (->> row-seq
         (map-indexed vector)
         (pfor/consume!
          (fn [[^long row-idx ^"[Ljava.lang.String;" row]]
            (when-not (and skip-bad-rows?
                           (not= (alength row) n-header-cols))
              (dotimes [col-idx (alength row)]
                (let [parser (col-idx->parser col-idx)]
                  (column-parsers/add-value! parser row-idx (aget row col-idx))))))))
    (parse-context/parsers->dataset options parsers)))


(defn partition-all-rows
  "Given a sequence of rows, partition into an undefined number of partitions of at most
  N rows but keep the header row as the first for all sequences."
  ([{:keys [header-row?]
     :or {header-row? true}} n row-seq]
   (let [[header-row row-seq] (if header-row?
                                [(first row-seq) (rest row-seq)]
                                [nil row-seq])
         row-partitions (partition-all n row-seq)]
     (if header-row
       (map #(concat [header-row] %) row-partitions)
       row-partitions))))


(defn sample-rows
  "Sample at most N rows selected randomly from the row sequence.  If sequence is
  shorter than length N will return less than N rows.
  Uses naive reservoir sampling:
  https://en.wikipedia.org/wiki/Reservoir_sampling"
  [{:keys [header-row?]
    :or {header-row? true}} n row-seq]
  (let [[header-row row-seq] (if header-row?
                               [(first row-seq) (rest row-seq)]
                               [nil row-seq])
        n (long n)
        ^objects row-data (into-array Object (take n row-seq))
        row-seq (seq (drop n row-seq))]
    ;;if we have more data than N
    (doseq [[idx row] (map-indexed vector row-seq)]
      (let [idx (+ (long idx) n)
            replace-idx (rand-int idx)]
        (when (< replace-idx n)
          (aset row-data replace-idx row))))
    (concat [header-row] row-data)))
