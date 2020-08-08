(ns  ^:no-doc tech.ml.dataset.categorical
  "Dealing with categorical dataset data involves having two mapping systems.
  The first is a map of category to integer within the same column.
  The second is a 'one-hot' encoding where you generate more columns but those have
  a reduced number of possible categories, usually one categorical value per
  column."
  (:require [tech.ml.protocols.dataset :as ds]
            [tech.ml.dataset.base :as ds-base]
            [tech.ml.protocols.column :as ds-col]
            [tech.ml.dataset.impl.column :as col-impl]
            [tech.v2.datatype :as dtype]
            [clojure.set :as c-set]
            [tech.ml.dataset.utils :as utils]
            [tech.parallel.for :as parallel-for])
  (:import [java.util HashMap Map]
           [java.util.function Function BiFunction BiConsumer]))


(def ^:private known-values
  {"true" 1
   "positive" 1
   "false" 0
   "negative" 0})


(defn make-string-table-from-table-args
  "Make a mapping of value->index from a list of either string values or [valname idx]
  pairs.
  Returns map of value->index."
  [table-value-list]
  ;; First, any explicit mappings are respected.
  (let [[str-table value-list]
        (reduce (fn [[str-table value-list] item]
                  (if (sequential? item)
                    [(assoc str-table
                            (first item)
                            (second item))
                     value-list]
                    [str-table (conj value-list item)]))
                [{} []]
                table-value-list)
        ;;Second, known values map so that true and false map reasonably.
        [str-table value-list]
        (reduce (fn [[str-table value-list] item]
                  (let [known-value (get known-values item)]
                    (if (and known-value
                             (not (contains? (set (vals str-table)) known-value)))
                      [(assoc str-table item known-value)
                       value-list]
                      [str-table (conj value-list item)])))
                [str-table []]
                value-list)]
    ;;Finally, auto-generate values for anything not mapped yet.
    (->> value-list
         (reduce (fn [str-table item]
                   (assoc str-table item
                          (first (remove (set (vals str-table))
                                         (range)))))
                 str-table))))


(defn make-string-table-from-column
  [column]
  (let [rdr (dtype/->reader column)
        final-map (HashMap.)
        final-combiner (reify Function
                         (apply [_ key]
                           (unchecked-long (.size final-map))))
        final-consumer (reify BiConsumer
                         (accept [_ key val]
                           (.computeIfAbsent final-map
                                             key final-combiner)))]
    (parallel-for/indexed-map-reduce
     (dtype/ecount rdr)
     (fn [^long start-idx ^long len]
       (let [retval (HashMap.)
             compute-op (reify Function
                          (apply [_ key]
                            (unchecked-long (.size retval))))]
         (dotimes [iter len]
           (.computeIfAbsent retval (rdr (unchecked-add iter start-idx))
                             compute-op))
         retval))
     ;;mapv here for side effects
     (partial mapv (fn [^Map lhs] (.forEach lhs final-consumer))))
    ;;Keep things as persistent maps so writing/reading to/from edn is good.
    (into {} final-map)))


(defn build-categorical-map
  "Given a dataset and these columns, produce a label-map of
  column-name to specific categorical label-map."
  [dataset column-name-seq & [table-value-list]]
  (let [provided-table (when (seq table-value-list)
                         (make-string-table-from-table-args table-value-list))]
    (->> column-name-seq
         (map (fn [column-name]
                [column-name (if provided-table
                               provided-table
                               (make-string-table-from-column
                                (ds/column
                                 dataset column-name)))]))
         (into {}))))


(defn column-categorical-map
  "Given a categorical map for a given column, produce a new column
  of the desired datatype with the values mapped to the table values."
  [categorical-map new-dtype old-column]
  (let [column-name (ds-col/column-name old-column)
        ^Map categorical-map (get categorical-map column-name)
        data-values
        (dtype/make-array-of-type
         new-dtype
         (dtype/reader-map
          #(let [val (.getOrDefault categorical-map % ::missing-value)]
             (when (= val ::missing-value)
               (throw (ex-info (format "Failed to find lookup for value %s"
                                       %)
                               {:item-value %
                                :possible-values (set (keys categorical-map))
                                :column-name column-name})))
             val)
          old-column)
         {:unchecked? true})]
    (col-impl/new-column column-name data-values
                         (assoc (ds-col/metadata old-column)
                                :label-map categorical-map))))


(defn inverse-map-categorical-col-fn
  [_src-column column-categorical-map]
  (let [inverse-map (c-set/map-invert column-categorical-map)]
    (fn [col-val]
      (if-let [col-label (get inverse-map (long col-val))]
        col-label
        (throw (ex-info
                (format "Failed to find label for column value %s"
                        col-val)
                {:inverse-label-map inverse-map}))))))


(defn inverse-map-categorical-columns
  [dataset src-column column-categorical-map]
  (let [column-values (-> (ds/column dataset src-column)
                          (dtype/->reader))]
    (->> column-values
         (mapv (inverse-map-categorical-col-fn
                src-column column-categorical-map)))))


(defn is-one-hot-label-map?
  [label-map]
  (let [[_col-val col-entry] (first label-map)]
    (not (number? col-entry))))


(defn build-one-hot-map
  [dataset column-name-seq & [one-hot-table-args]]
  (->> column-name-seq
       (map (fn [colname]
              (when-not (#{:string :keyword :symbol}
                         (dtype/get-datatype (ds/column dataset colname)))
                (throw (ex-info (format "One hot applied to non string column: %s(%s)"
                                        colname (dtype/get-datatype
                                                 (ds/column dataset colname)))
                                {})))
              (let [col-vals (ds-col/unique (ds/column dataset colname))
                    one-hot-arg (or one-hot-table-args (vec col-vals))]
                [colname
                 (cond
                   (sequential? one-hot-arg)
                   (->> one-hot-arg
                        (map (fn [argval]
                               [argval [(utils/extend-column-name colname argval) 1]]))
                        (into {}))
                   (map? one-hot-arg)
                   (let [valseq (->> (vals one-hot-arg)
                                     (mapcat (fn [arglist]
                                               (if (keyword? arglist)
                                                 [arglist]
                                                 arglist))))
                         contains-rest? (->> valseq
                                             (filter #(= :rest %))
                                             first)
                         stated-vals (->> valseq
                                          (clojure.core/remove #(= :rest %))
                                          set)
                         leftover (c-set/difference col-vals stated-vals)]
                     (when-not (or (not= (count leftover) 0)
                                   contains-rest?)
                       (throw (ex-info (format "Column values not accounted for: %s"
                                               (vec leftover))
                                       {:stated-values stated-vals
                                        :leftover leftover})))
                     (->> one-hot-arg
                          (mapcat (fn [[arg-key argval-seq]]
                                    (let [argval-seq (->> (if (= :rest argval-seq)
                                                            [argval-seq]
                                                            argval-seq)
                                                          (mapcat (fn [argval]
                                                                    (if (= :rest argval)
                                                                      (vec leftover)
                                                                      [argval]))))
                                          local-colname (utils/extend-column-name
                                                         colname arg-key)]
                                      (->> argval-seq
                                           (map-indexed (fn [idx argval]
                                                          [argval [local-colname
                                                                   (inc idx)]]))))))
                          (into {})))
                   :else
                   (throw (ex-info (format "Unrecognized one hot argument: %s"
                                           one-hot-arg) {})))])))
       (into {})))


(defn column-one-hot-map
  "Using one hot map, produce Y new columns while removing existing column."
  [one-hot-map new-dtype dataset column-name]
  (let [column (ds/column dataset column-name)
        dataset (ds/remove-column dataset column-name)
        context (get one-hot-map column-name)
        col-values (dtype/->reader column)
        new-column-map (->> context
                            (map (fn [[_argval [new-column-name _colval]]]
                                   [new-column-name
                                    (dtype/make-array-of-type
                                     new-dtype (dtype/ecount column))]))
                            (into {}))]
    (->> col-values
         (map-indexed vector)
         (pmap
          (fn [[idx col-val]]
            (if-let [table-entry (get context col-val)]
              (let [[new-column-name new-colval] table-entry]
                (if-let [new-col (get new-column-map new-column-name)]
                  (dtype/set-value! new-col idx new-colval)
                  (throw (ex-info (format "Failed to find new column: %s"
                                          new-column-name)
                                  {:new-columns (keys new-column-map)}))))
              (throw (ex-info (format "Failed to find string table value: %s"
                                      col-val)
                              {:column-value col-val
                               :column-one-hot-table context})))))
         dorun)
    (->> new-column-map
         (reduce (fn [dataset [column-name column-data]]
                   (ds/add-column
                    dataset
                    (col-impl/new-column column-name column-data
                                       (assoc
                                        (ds-col/metadata column)
                                        :label-map context))))
                 dataset))))


(defn- inverse-map-one-hot-column-values-fn
  [src-column column-label-map]
  (let [inverse-map (c-set/map-invert column-label-map)
        colname-seq (->> inverse-map
                         keys
                         (map first)
                         distinct)]
    (fn [& col-values]
      (let [nonzero-entries
            (->> (map (fn [col-name col-val]
                        (when-not (= 0 (long col-val))
                          [col-name (long col-val)]))
                      colname-seq col-values)
                 (remove nil?))]
        (when-not (= 1 (count nonzero-entries))
          (throw (ex-info
                  (format "Multiple (or zero) nonzero entries detected:[%s]%s"
                          src-column nonzero-entries)
                  {:column-name src-column
                   :label-map column-label-map})))
        (if-let [colval (get inverse-map (first nonzero-entries))]
          colval
          (throw (ex-info (format "Failed to find column entry %s: %s"
                                  (first nonzero-entries)
                                  (keys inverse-map))
                          {:entry-label (first nonzero-entries)
                           :label-map column-label-map})))))))


(defn- inverse-map-one-hot-columns
  [dataset src-column column-label-map]
  (let [colname-seq (->> column-label-map
                         vals
                         (map first)
                         distinct)]
    (->> (ds/select dataset colname-seq :all)
         (ds-base/value-reader)
         (map #(apply (inverse-map-one-hot-column-values-fn
                       src-column column-label-map)
                      %)))))


(defn column-values->categorical
  "Given a column encoded via either string->number or one-hot, reverse
  map to the a sequence of the original string column values."
  [dataset src-column categorical-map]
  (when-not (contains? categorical-map src-column)
    (throw (ex-info (format "Failed to find column %s in label map %s"
                            src-column categorical-map)
                    {:column-name src-column
                     :label-map categorical-map})))

  (let [label-map (get categorical-map src-column)]
    (if (is-one-hot-label-map? label-map)
      (inverse-map-one-hot-columns dataset src-column label-map)
      (inverse-map-categorical-columns dataset src-column label-map))))
