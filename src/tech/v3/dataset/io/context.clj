(ns tech.v3.dataset.io.context
  (:require [tech.v3.dataset.io.column-parsers :as column-parsers]
            [tech.v3.datatype :as dtype]
            [tech.v3.dataset.impl.dataset :as ds-impl])
  (:import [java.util.function Function]
           [java.util HashMap]))


(defn options->parser-fn
  "Given the (beast of an) options map used for parsing, run through it
  and created the specific parse context.  A parse context is a function
  that produces a column parser for a given column name or index.
  parse-type is either :string or :object."
  [options parse-type]
  (let [default-parse-fn (case parse-type
                           :object column-parsers/promotional-object-parser
                           :string column-parsers/promotional-string-parser
                           nil (constantly nil))
        key-fn (or (:key-fn options) identity)
        parser-descriptor (:parser-fn options)]
    (fn [cname-or-index]
      (let [cname (if (number? cname-or-index)
                      (long cname-or-index)
                      (key-fn cname-or-index))]
        (cond
          (nil? parser-descriptor)
          (default-parse-fn cname)
          (map? parser-descriptor)
          (if-let [col-parser-desc (or (get parser-descriptor cname)
                                       (get parser-descriptor cname-or-index))]
            (column-parsers/make-fixed-parser cname col-parser-desc)
            (default-parse-fn cname))
          :else
          (column-parsers/make-fixed-parser cname parser-descriptor))))))


(defn- make-colname
  [rd]
  (if (number? rd)
    (str "column-" rd)
    rd))


(defn options->col-idx-parse-context
  "Given an option map and a parse type, return a map of parsers
  and a function to get a parser from a given column idx.
  returns:
  {:parsers - parsers
   :col-idx->parser - given a column idx, get a parser.  Mutates parsers."
  [options parse-type col-idx->colname]
  (let [parse-context (options->parser-fn options parse-type)
        parsers (HashMap.)
        key-fn (:key-fn options identity)
        colparser-compute-fn (reify Function
                               (apply [this col-idx]
                                 (let [colname (or (col-idx->colname col-idx)
                                                   (make-colname col-idx))]
                                   {:column-idx col-idx
                                    :column-name (key-fn colname)
                                    :column-parser (parse-context colname)})))
        col-idx->parser (fn [col-idx]
                          (:column-parser
                           (.computeIfAbsent parsers (long col-idx)
                                             colparser-compute-fn)))]
    {:parsers parsers
     :col-idx->parser col-idx->parser}))


(defn parsers->dataset
  ([options parsers row-count]
   (let [all-parsers (vals parsers)
         row-count (long row-count)]
     (->> all-parsers
          (sort-by :column-idx)
          (mapv (fn [{:keys [column-name column-parser]}]
                  (assoc (column-parsers/finalize! column-parser row-count)
                         :name column-name)))
          (ds-impl/new-dataset options))))
  ([options parsers]
   (parsers->dataset options parsers
                     (apply max 0 (map (comp dtype/ecount :column-parser)
                                       (vals parsers))))))
