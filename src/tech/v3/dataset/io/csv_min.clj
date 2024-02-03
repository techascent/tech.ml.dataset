(ns tech.v3.dataset.io.csv-min
  (:require [tech.v3.dataset.io.context :as parse-context]
            [tech.v3.dataset.io.column-parsers :as column-parsers]
            [charred.coerce :as coerce]
            [charred.api :as charred]
            [ham-fisted.reduce :as hamf-rf]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defn rows->dataset-fn
  "Create an efficiently callable function to parse row-batches into datasets.
  Returns function from row-iter->dataset.  Options passed in here are the
  same as ->dataset."
  [{:keys [header-row?]
    :or {header-row? true}
    :as options}]
  (fn [row-iter]
    (let [row-iter (coerce/->iterator row-iter)
          header-row (if (and header-row? (.hasNext row-iter))
                       (vec (.next row-iter))
                       [])
          n-header-cols (count header-row)
          {:keys [parsers col-idx->parser]}
          (parse-context/options->col-idx-parse-context
           options :string (fn [^long col-idx]
                             (when (< col-idx n-header-cols)
                               (header-row col-idx))))
          n-records (get options :n-records (get options :num-rows))]
      ;;initialize parsers so if there are no more rows we get a dataset with
      ;;at least column names
      (dotimes [idx n-header-cols]
        (col-idx->parser idx))

      (if n-records
        (let [n-records (long n-records)]
          (loop [continue? (.hasNext row-iter)
                 row-idx 0]
            (if continue?
              (do
                (reduce (hamf-rf/indexed-accum
                         acc col-idx field
                         (-> (col-idx->parser col-idx)
                             (column-parsers/add-value! row-idx field)))
                        nil
                        (.next row-iter))
                (recur (and (.hasNext row-iter)
                            (< (inc row-idx) n-records))
                       (unchecked-inc row-idx)))
              (parse-context/parsers->dataset options parsers))))
        (loop [continue? (.hasNext row-iter)
               row-idx 0]
          (if continue?
            (do
              (reduce (hamf-rf/indexed-accum
                       acc col-idx field
                       (-> (col-idx->parser col-idx)
                           (column-parsers/add-value! row-idx field)))
                      nil
                      (.next row-iter))
              (recur (.hasNext row-iter) (unchecked-inc row-idx)))
            (parse-context/parsers->dataset options parsers)))))))


(defn csv->dataset
  "Read a csv into a dataset.  Same options as [[tech.v3.dataset/->dataset]]."
  [input-path & [options]]
  (let [s (charred/read-csv-supplier (java.io.File. (str input-path))
                                     (merge {:profile :mutable} options))]
    (try
      (let [iter (if-let [n-initial-skip-rows (get options :n-initial-skip-rows)]
                   (let [iter (coerce/->iterator s)]
                     (dotimes [idx n-initial-skip-rows]
                       (when (.hasNext iter) (.next iter)))
                     iter)
                   s)]
        ((rows->dataset-fn options) iter))
      (finally 
        (when (instance? java.lang.AutoCloseable s)
          (.close ^java.lang.AutoCloseable s))))))
