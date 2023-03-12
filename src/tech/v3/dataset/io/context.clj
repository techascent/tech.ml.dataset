(ns tech.v3.dataset.io.context
  (:require [tech.v3.dataset.io.column-parsers :as column-parsers]
            [tech.v3.dataset.protocols :as ds-proto]
            [tech.v3.datatype :as dtype]
            [tech.v3.dataset.impl.dataset :as ds-impl]
            [tech.v3.dataset.impl.column :as col-impl]
            [ham-fisted.api :as hamf])
  (:import [java.util.function Function]
           [java.util Map]
           [tech.v3.datatype ObjectBuffer ArrayHelpers]
           [tech.v3.dataset.impl.dataset Dataset]))


(set! *warn-on-reflection* true)


(defn options->parser-fn
  "Given the (beast of an) options map used for parsing, run through it
  and created the specific parse context.  A parse context is a function
  that produces a column parser for a given column name or index.
  parse-type is either :string or :object."
  [options parse-type]
  (let [default-parse-fn (case (get options :parser-type parse-type)
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
          (default-parse-fn cname options)
          (map? parser-descriptor)
          (if-let [col-parser-desc (or (get parser-descriptor cname)
                                       (get parser-descriptor cname-or-index))]
            (column-parsers/make-fixed-parser cname col-parser-desc options)
            (default-parse-fn cname options))
          :else
          (column-parsers/make-fixed-parser cname parser-descriptor options))))))


(defn- make-colname
  [rd]
  (if (number? rd)
    (str "column-" rd)
    rd))


(deftype ObjectArrayList [^{:unsynchronized-mutable true
                            :tag 'objects} data]
  ObjectBuffer
  (lsize [_this] (alength ^objects data))
  (writeObject [_this idx value]
    (when (>= idx (alength ^objects data))
      (let [old-len (alength ^objects data)
            new-len (* 2 idx)
            new-data (object-array new-len)]
        (System/arraycopy data 0 new-data 0 old-len)
        (set! data new-data)))
    (ArrayHelpers/aset ^objects data idx value))
  (readObject [_this idx]
    (when (< idx (alength ^objects data))
      (aget ^objects data idx))))


(defn options->col-idx-parse-context
  "Given an option map and a parse type, return a map of parsers
  and a function to get a parser from a given column idx.
  returns:
  {:parsers - parsers
   :col-idx->parser - given a column idx, get a parser.  Mutates parsers."
  [options parse-type col-idx->colname]
  (let [parse-context (options->parser-fn options parse-type)
        parsers (ObjectArrayList. (object-array 16))
        key-fn (:key-fn options identity)
        colparser-compute-fn (reify Function
                               (apply [this col-idx]
                                 (let [colname (col-idx->colname col-idx)
                                       colname (if (empty? colname)
                                                 (make-colname col-idx)
                                                 colname)]
                                   {:column-idx col-idx
                                    :column-name (key-fn colname)
                                    :column-parser (parse-context colname)})))
        col-idx->parser (fn [col-idx]
                          (let [col-idx (long col-idx)]
                            (if-let [parser (.readObject parsers col-idx)]
                              (parser :column-parser)
                              (let [parser (.apply colparser-compute-fn col-idx)]
                                (.writeObject parsers col-idx parser)
                                (parser :column-parser)))))]
    {:parsers parsers
     :col-idx->parser col-idx->parser}))


(defn parsers->dataset
  "Parsers are a sequence of maps of :column-name :column-parser; :columns-parser is an object that implement
  tech.v3.dataset.io.column-parser.PParser/finalize! which returns the column data in map form
  preferrably with :force-datatype? to avoid rescanning any data."
  ([options parsers row-count]
   (let [row-count (long row-count)
         columns (hamf/mut-list)
         cmap (hamf/mut-map)
         dname (or (get options :name)
                   (get options :dataset-name)
                   "_unnamed")]
     (transduce (remove nil?)
                (fn
                  ([] nil)
                  ([acc] acc)
                  ([acc ^Map parse-map]
                   (let [column-name (.get parse-map :column-name)
                         column-parser (.get parse-map :column-parser)]
                     (.put cmap column-name (.size columns))
                     (.add columns (-> (assoc (column-parsers/finalize! column-parser row-count)
                                              :tech.v3.dataset/name column-name)
                                       (col-impl/new-column))))))
             (if (instance? Map parsers)
               (vals parsers)
               parsers))
     (Dataset. (persistent! columns) (persistent! cmap) {:name dname} 0 0)))
  ([options parsers]
   (parsers->dataset options parsers
                     (apply max 0 (map (comp dtype/ecount :column-parser)
                                       parsers)))))
