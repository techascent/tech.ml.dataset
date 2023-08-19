(ns tech.v3.dataset.io.csv
  "CSV parsing based on [charred.api/read-csv](https://cnuernber.github.io/charred/)."
  (:require [charred.api :as charred]
            [charred.bulk :as bulk]
            [charred.coerce :as coerce]
            [tech.v3.dataset.io :as ds-io]
            [tech.v3.parallel.for :as pfor]
            [tech.v3.datatype :as dtype]
            [tech.v3.io :as io]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.dataset.io.column-parsers :as column-parsers]
            [tech.v3.dataset.io.context :as parse-context]
            [tech.v3.dataset.impl.dataset :as ds-impl]
            [tech.v3.dataset.protocols :as ds-proto]
            [ham-fisted.api :as hamf]
            [ham-fisted.reduce :as hamf-rf]
            [ham-fisted.lazy-noncaching :as lznc])
  (:import [tech.v3.datatype ArrayHelpers]
           [clojure.lang IReduceInit]
           [java.lang AutoCloseable]
           [java.util Iterator]))


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



(defn csv->dataset-seq
  "Read a csv into a lazy sequence of datasets.  All options of [[tech.v3.dataset/->dataset]]
  are suppored aside from `:n-initial-skip-rows` with an additional option of
  `:batch-size` which defaults to 128000.

  Options are passed through to
  [charred.bulk/batch-csv-rows](https://cnuernber.github.io/charred/charred.bulk.html#var-batch-csv-rows)
  renaming where necessary.  This method defaults to using a load thread - see above method
  for more options.  To disable the load thread use `:csv-load-thread-name nil`.

  When using multithreaded loading, options are also passed through to
  [ham-fisted.api/pmap-opts](https://cnuernber.github.io/ham-fisted/ham-fisted.api.html#var-pmap-opts)
  so you can change the amount of `:n-lookahead` the pmap opteration uses when submitting jobs
  to the thread pool.  By default this is set to 4 to decrease possible OOM sitations.

  The input will only be closed once the entire sequence is realized.

  Options:

  * :dataset-tfn - dataset->x transformation function to be performed on
    in the same thread context just after dataset is loaded.  Doing some operations
    in this transform function can be considerably more efficient than only loading
    the dataset when using multithreaded loading."
  [input & [options]]
  (let [options (merge {:profile :mutable
                        :csv-load-thread-name "TMD CSV load thread"}
                       options)
        load-fn (rows->dataset-fn options)
        threaded? (boolean (get options :csv-load-thread-name))
        batches (->> (charred/read-csv-supplier (ds-io/input-stream-or-reader input) options)
                     (bulk/batch-csv-rows (get options :batch-size 128000) options))
        load-fn (if-let [tfn (get options :csv-load-tfn)]
                  #(tfn (load-fn %))
                  load-fn)]
    (if threaded?
      (hamf/pmap-opts (merge {:n-lookahead (get options :csv-load-queue-size 4)}
                             options)
                      load-fn
                      batches)
      (map load-fn batches))))


(defn csv->dataset
  "Read a csv into a dataset.  Same options as [[tech.v3.dataset/->dataset]]."
  [input & [options]]
  (let [s (charred/read-csv-supplier (ds-io/input-stream-or-reader input)
                                     (merge {:profile :mutable} options))
        iter (if-let [n-initial-skip-rows (get options :n-initial-skip-rows)]
               (let [iter (coerce/->iterator s)]
                 (dotimes [idx n-initial-skip-rows]
                   (when (.hasNext iter) (.next iter)))
                 iter)
               s)
        retval ((rows->dataset-fn options) iter)]
    (when (instance? AutoCloseable s)
      (.close ^AutoCloseable s))
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


(defn rows->csv!
  "Given an something convertible to an output stream, an optional set of headers
  as string arrays, and a sequence of string arrows, write a CSV or a TSV file.

  Options:

  * `:separator` - Defaults to \tab.
  * `:quote` - Default \\\"
  * `:quote?` A predicate function which determines if a string should be quoted.
        Defaults to quoting only when necessary.  May also be the the value 'true' in which
        case every field is quoted.
  *  :newline - `:lf` (default) or `:cr+lf`.
  *  :close-writer? - defaults to true.  When true, close writer when finished."
  ([output headers rows]
   (rows->csv! output headers rows {}))
  ([output headers rows
    {:keys [separator]
     :or {separator \tab}
     :as options}]
   (apply charred/write-csv (io/writer! output)
          (if headers (lznc/concat [headers] rows) rows)
          (apply concat (seq (merge {:close-writer? true
                                     :separator separator}
                                    options))))))


(defn- data->string
  ^String [data-item]
  (when-not (nil? data-item)
    (cond
      (string? data-item) data-item
      (keyword? data-item) (name data-item)
      (symbol? data-item) (name data-item)
      :else (.toString ^Object data-item))))


(defn- write-csv!
  "Write a dataset to a tsv or csv output stream.  Closes output if a stream
  is passed in.  File output format will be inferred if output is a string -
    - .csv, .tsv - switches between tsv, csv.  Tsv is the default.
    - *.gz - write to a gzipped stream.

  options:

  * `:separator` - in case output isn't a string, you can use either \\, or \\tab to switch
    between csv or tsv output respectively.
  * `:headers?` - if csv headers are written, defaults to true.
  * `:gzipped?` - When true, use a gizpped output stream.
  * `:file-type` - `:csv` or `:tsv`."
  ([ds output options]
   (let [{:keys [gzipped? file-type]}
         (merge
          (when (string? output)
            (ds-io/str->file-info output))
          options)
         headers (when (get options :headers? true)
                   (map (comp data->string :name meta) (vals ds)))
         rows (->> (ds-proto/rowvecs ds nil)
                   (lznc/map #(lznc/map data->string %)))
         tsv? (or (= file-type :tsv) (= \tab (:separator options)))
         output (if gzipped?
                  (io/gzip-output-stream! output)
                  output)]
     (rows->csv! output headers rows (assoc options :separator (if tsv? \tab \,)))))
  ([ds output]
   (write-csv! ds output {})))


(defmethod ds-io/dataset->data! :csv
  [dataset output options]
  (write-csv! dataset output options))


(defmethod ds-io/dataset->data! :tsv
  [dataset output options]
  (write-csv! dataset output (assoc options :separator \tab)))


(defmethod ds-io/dataset->data! :txt
  [dataset output options]
  (write-csv! dataset output (assoc options :separator \tab)))


(comment

  (do
    (import '[java.util.zip ZipFile ZipInputStream])
    (import '[java.util.concurrent ArrayBlockingQueue])
    (require '[charred.api :as charred])
    (require '[charred.bulk :as bulk])

    (defn- abq->iterable
      [^ArrayBlockingQueue abq]
      (reify
        Iterable
        (iterator [this]
          (println "iterator requested")
          (let [nv* (volatile! (.take abq))]
            (reify
              java.util.Iterator
              (hasNext [this] (not= ::finished @nv*))
              (next [this]
                (let [nv @nv*]
                  (vreset! nv* (.take abq))
                  nv)))))
        IReduceInit
        (reduce [this rfn acc]
          (println "reduce requested")
          (loop [acc acc
                 nv (.take abq)]
            (if (or (identical? ::finished nv)
                    (not (reduced? acc)))
              (recur (rfn acc nv) (.take abq))
              (if (reduced? acc)
                @acc
                acc))))))

    (defn load-zip
      [fname]
      (let [zf (ZipInputStream. (io/input-stream fname))
            fe (.getNextEntry zf)
            _ (println (format "Found %s" (.getName fe)))
            parse-fn (rows->dataset-fn nil)]
        (reduce (fn [rc batch]
                  (+ rc (ds-proto/row-count (parse-fn batch))))
                0
                (bulk/batch-csv-rows 10000 (charred/read-csv-supplier zf)))))


    (defn load-zip-parallel
      [fname]
      (let [zf (ZipInputStream. (io/input-stream fname))
            fe (.getNextEntry zf)
            _ (println (format "Found %s" (.getName fe)))
            s (charred/read-csv-supplier zf)
            row-batches (bulk/batch-csv-rows 10000 s)
            batch-queue (ArrayBlockingQueue. 16)
            n-parse-threads 6
            csv-thread
            (Thread. ^java.lang.Runnable
                     (fn []
                       (let [rc
                             (reduce (fn [rc row-batch]
                                       (let [data (vec row-batch)
                                             rc (+ rc (count data))]
                                         (.put batch-queue data)
                                         rc))
                                     0
                                     row-batches)]
                         (.put batch-queue ::finished)
                         (println "csv parse thread finished")))
                     "CSV parse thread")
            _ (.start csv-thread)
            sum (->> (abq->iterable batch-queue)
                     (hamf/pmap #(ds-proto/row-count (first (rows->dataset-seq nil %))))
                     (reduce + 0))]
        (.join csv-thread)
        (println "csv thread joined")
        sum))

    )

  (def result (load-zip "/home/chrisn/Downloads/bigcsv/full_data.zip"))


  (def result (load-zip-parallel "/home/chrisn/Downloads/bigcsv/full_data.zip"))



  )
