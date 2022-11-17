(ns tech.v3.dataset.join
  "implementation of join algorithms, both exact (hash-join) and near."
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.packing :as packing]
            [tech.v3.datatype.argops :as argops]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.datetime :as dtype-dt]
            [tech.v3.parallel.for :as parallel-for]
            [tech.v3.dataset.column :as ds-col]
            [tech.v3.dataset.base :as ds-base]
            [tech.v3.dataset.readers :as ds-readers]
            [tech.v3.dataset.impl.dataset :as ds-impl]
            [tech.v3.dataset.utils :as ds-utils]
            [com.github.ztellman.primitive-math :as pmath]
            [clojure.set :as set])
  (:import [tech.v3.datatype ObjectReader Buffer
            BinaryPredicate BinaryOperator
            BinaryOperators$DoubleBinaryOperator]
           [ham_fisted IMutList]
           [java.util List HashSet Map]))


(set! *warn-on-reflection* true)


(defn- colname->str
  ^String [item]
  (ds-utils/column-safe-name item))


(defn- similar-colname-type
  [orign-name new-name]
  (cond
    (string? orign-name) new-name
    (keyword? orign-name) (keyword new-name)
    (symbol? orign-name) (symbol new-name)
    :else
    new-name))


(defn- colname->lhs-rhs-colnames
  [colname]
  (if (instance? java.util.Collection colname)
    colname
    [colname colname]))


(declare finalize-join-result)


(defn- add-all!
  [^List target ^List new-data]
  (.addAll target new-data)
  target)

(defn- default-table-name
  [ds-table default-val]
  (if (= (ds-base/dataset-name ds-table) "_unnamed")
    default-val
    (ds-base/dataset-name ds-table)))

(defn- nice-column-names
  [& table-name-column-seq-pairs]
  (->>
   table-name-column-seq-pairs
   (mapcat (fn [[table-name column-seq]]
             (map vector
                  column-seq
                  (repeat table-name))))
   (reduce
    (fn [[final-columns name-set] [column prefix]]
      (let [original-colname (ds-col/column-name column)
            colname
            (loop [found? (contains? name-set
                                     original-colname)
                   new-col-name original-colname
                   idx 0]
              (if found?
                (let [new-col-name
                      (if (== 0 idx)
                        (str prefix "."
                             (colname->str original-colname))
                        (str prefix "." original-colname "-" idx))]
                  (recur (contains? name-set new-col-name)
                         new-col-name
                         (unchecked-inc idx)))
                (similar-colname-type original-colname
                                      new-col-name)))]
        [(conj final-columns (with-meta column
                               (assoc (meta column)
                                      :name colname
                                      :src-table-name prefix
                                      :previous-colname original-colname)))
         (conj name-set colname)]))
    [[] #{}])
   (first)))


(defn- update-join-metadata
  [result-table lhs-table-name rhs-table-name]
  (let [name-data (->> (map meta (vals result-table))
                       (group-by :src-table-name))]
    (with-meta result-table
      (assoc (meta result-table)
             :left-column-names
             (->> (get name-data lhs-table-name)
                  (map (juxt :previous-colname :name))
                  (into {}))
             :right-column-names
             (->> (get name-data rhs-table-name)
                  (map (juxt :previous-colname :name))
                  (into {}))))))


(defn- fix-inner-join-metadata
  [result-table lhs-join-colname rhs-join-colname]
  (with-meta result-table
    (assoc-in (meta result-table)
              [:right-column-names rhs-join-colname]
              lhs-join-colname)))


(defn- finalize-join-result
  [lhs-colname rhs-colname lhs rhs lhs-indexes rhs-indexes lhs-missing rhs-missing]
  (let [lhs-columns (ds-base/columns (ds-base/remove-column lhs lhs-colname))
        rhs-columns (ds-base/columns (ds-base/remove-column rhs rhs-colname))
        lhs-table-name (default-table-name lhs "left")
        rhs-table-name (default-table-name rhs "right")
        lhs-join-column (lhs lhs-colname)
        rhs-join-column (rhs rhs-colname)]
    (merge
     {:inner
       (let [lhs-columns (map #(ds-col/select % lhs-indexes) lhs-columns)
             rhs-columns (map #(ds-col/select % rhs-indexes) rhs-columns)]
         (-> (ds-impl/new-dataset
              "inner-join"
              (nice-column-names
               [lhs-table-name (concat [(ds-col/select lhs-join-column lhs-indexes)]
                                       lhs-columns)]
               [rhs-table-name rhs-columns]))
             (update-join-metadata lhs-table-name rhs-table-name)
             (fix-inner-join-metadata lhs-colname rhs-colname)))
       :lhs-indexes lhs-indexes
       :rhs-indexes rhs-indexes}
      (when rhs-missing
        {:rhs-missing rhs-missing
         :right-outer
         (let [n-empty (count rhs-missing)
               rhs-indexes (add-all! (dtype/clone rhs-indexes) rhs-missing)
               rhs-columns (map #(ds-col/select % rhs-indexes) rhs-columns)
               lhs-columns
               (->> (concat [lhs-join-column] lhs-columns)
                    (map (fn [old-col]
                           (ds-col/extend-column-with-empty
                            (ds-col/select old-col lhs-indexes)
                            n-empty))))]
           (-> (ds-impl/new-dataset
                "right-outer-join"
                (nice-column-names
                 [lhs-table-name lhs-columns]
                 [rhs-table-name (concat [(ds-col/select rhs-join-column rhs-indexes)]
                                         rhs-columns)]))
               (update-join-metadata lhs-table-name rhs-table-name)))})
      (when lhs-missing
        {:lhs-missing lhs-missing
         :left-outer
         (let [n-empty (count lhs-missing)
               lhs-indexes (add-all! (dtype/clone lhs-indexes) lhs-missing)
               lhs-columns (map #(ds-col/select % lhs-indexes) lhs-columns)
               rhs-columns
               (->> (concat [rhs-join-column] rhs-columns)
                    (map (fn [old-col]
                           (ds-col/extend-column-with-empty
                            (ds-col/select old-col rhs-indexes)
                            n-empty))))]
           (-> (ds-impl/new-dataset
                "left-outer-join"
                (nice-column-names
                 [lhs-table-name (concat [(ds-col/select lhs-join-column lhs-indexes)]
                                         lhs-columns)]
                 [rhs-table-name rhs-columns]))
               (update-join-metadata lhs-table-name rhs-table-name)))}))))


(defn- hash-join-algo
  [lhs-col rhs-col lhs-missing? rhs-missing? operation-space]
  (let [lhs-dtype (packing/unpack-datatype (dtype/elemwise-datatype lhs-col))
        rhs-dtype (packing/unpack-datatype (dtype/elemwise-datatype rhs-col))
        op-dtype (casting/simple-operation-space
                   (casting/widest-datatype lhs-dtype rhs-dtype))
        ;;Ensure we group in same space if possible.
        ^Map idx-groups (argops/arggroup
                          (dtype/elemwise-cast lhs-col op-dtype))
        rhs-col (dtype/->reader (dtype/elemwise-cast rhs-col op-dtype))
        n-elems (dtype/ecount rhs-col)
        {lhs-indexes :lhs-indexes
         rhs-indexes :rhs-indexes
         rhs-missing :rhs-missing
         lhs-found :lhs-found}
        (parallel-for/indexed-map-reduce
          n-elems
          (fn [^long outer-idx ^long n-indexes]
            (let [lhs-indexes (dtype/make-list operation-space)
                  rhs-indexes (dtype/make-list operation-space)
                  rhs-missing (dtype/make-list operation-space)
                  lhs-found (HashSet.)]
              (dotimes [inner-idx n-indexes]
                (let [idx (+ outer-idx inner-idx)
                      rhs-val (.readObject rhs-col idx)]
                  (if-let [^IMutList item (.get idx-groups rhs-val)]
                    (do
                      (when lhs-missing? (.add lhs-found rhs-val))
                      (dotimes [_n-iters (.size item)] (.add rhs-indexes idx))
                      (.addAll lhs-indexes item))
                    (when rhs-missing? (.add rhs-missing idx)))))
              {:lhs-indexes lhs-indexes
               :rhs-indexes rhs-indexes
               :rhs-missing rhs-missing
               :lhs-found lhs-found}))
          (partial reduce (fn [accum nextmap]
                            (->> accum
                                 (map (fn [[k v]]
                                        (if (= k :lhs-found)
                                          (.addAll ^HashSet v
                                                   ^HashSet (nextmap k))
                                          (.addAll ^List v
                                                   ^List (nextmap k)))
                                        [k v]))
                                 (into {})))))
         lhs-missing (when lhs-missing?
                       (reduce (fn [lhs-missing lhs-missing-key]
                                 (.addAll ^List lhs-missing
                                          ^List (get idx-groups lhs-missing-key))
                                 lhs-missing)
                               (dtype/make-list operation-space)
                               (->> (keys idx-groups)
                                    (remove #(.contains ^HashSet lhs-found %)))))]
    {:lhs-indexes lhs-indexes
     :rhs-indexes rhs-indexes
     :lhs-missing lhs-missing
     :rhs-missing rhs-missing}))



(defn hash-join
  "Join by column.  For efficiency, lhs should be smaller than rhs.
  colname - may be a single item or a tuple in which is destructures as:
     (let [[lhs-colname rhs-colname]] colname] ...)
  An options map can be passed in with optional arguments:
  :lhs-missing? Calculate the missing lhs indexes and left outer join table.
  :rhs-missing? Calculate the missing rhs indexes and right outer join table.
  :operation-space - either :int32 or :int64.  Defaults to :int32.
  Returns
  {:join-table - joined-table
   :lhs-indexes - matched lhs indexes
   :rhs-indexes - matched rhs indexes
   ;; -- when rhs-missing? is true --
   :rhs-missing - missing indexes of rhs.
   :rhs-outer-join - rhs outer join table.
   ;; -- when lhs-missing? is true --
   :lhs-missing - missing indexes of lhs.
   :lhs-outer-join - lhs outer join table.
  }"
  ([colname lhs rhs]
   (hash-join colname lhs rhs {}))
  ([colname lhs rhs {:keys [operation-space]
                     :or {operation-space :int32}
                     :as options}]
   (let [[lhs-colname rhs-colname] (colname->lhs-rhs-colnames colname)

         lhs-col (lhs lhs-colname)
         rhs-col (rhs rhs-colname)
         lhs-missing? (:lhs-missing? options)
         rhs-missing? (:rhs-missing? options)
         {:keys [lhs-indexes rhs-indexes lhs-missing rhs-missing]}
         (hash-join-algo lhs-col rhs-col lhs-missing? rhs-missing? operation-space)]
     (finalize-join-result lhs-colname rhs-colname lhs rhs
                           lhs-indexes rhs-indexes
                           lhs-missing
                           rhs-missing))))


(defn inner-join
  "Inner join by column.  For efficiency, lhs should be smaller than rhs.
   colname - may be a single item or a tuple in which is destructures as:
     (let [[lhs-colname rhs-colname]] colname] ...)
  An options map can be passed in with optional arguments:
  :operation-space - either :int32 or :int64.  Defaults to :int32.
  Returns the joined table"
  ([colname lhs rhs]
   (inner-join colname lhs rhs {}))
  ([colname lhs rhs options]
   (-> (hash-join colname lhs rhs options)
       :inner)))


(defn right-join
  "Right join by column.  For efficiency, lhs should be smaller than rhs.
  colname - may be a single item or a tuple in which is destructures as:
     (let [[lhs-colname rhs-colname]] colname] ...)
  An options map can be passed in with optional arguments:
  :operation-space - either :int32 or :int64.  Defaults to :int32.
  Returns the joined table"
  ([colname lhs rhs]
   (right-join colname lhs rhs {}))
  ([colname lhs rhs options]
   (-> (hash-join colname lhs rhs (assoc options :rhs-missing? options))
       :right-outer)))


(defn left-join
  "Left join by column.  For efficiency, lhs should be smaller than rhs.
   colname - may be a single item or a tuple in which is destructures as:
     (let [[lhs-colname rhs-colname]] colname] ...)
  An options map can be passed in with optional arguments:
  :operation-space - either :int32 or :int64.  Defaults to :int32.
  Returns the joined table"
  ([colname lhs rhs]
   (left-join colname lhs rhs {}))
  ([colname lhs rhs options]
   (-> (hash-join colname lhs rhs (assoc options :lhs-missing? options))
       :left-outer)))


(defn pd-merge
  "Pandas-style [merge](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.merge.html).
  This is similar to join except it can merge on multiple columns for the left and right
  sides.

  Options:

  * `:on` - column name or list of columns names.  Names must be found in both datasets.
  * `:left-on` - Column name or list of column names
  * `:right-on` - Column name or list of column names
  * `:how` - left, right inner, outer, cross.  If cross, then no on, left-on, right-on can
     be provided.

  Examples:

```clojure
user> (require '[tech.v3.dataset :as ds])
nil
user> (require '[tech.v3.dataset.join :as ds-join])
nil
user> (def ds-a (ds/->dataset {:a [:a :b :b :a :c]
                            :b (range 5)
                            :c (range 5)}))
#'user/ds-a
user> (def ds-b (ds/->dataset {:a [:a :b :a :b :d]
                            :b (range 5)
                            :c (range 6 11)}))
#'user/ds-b
user> ds-a
_unnamed [5 3]:

| :a | :b | :c |
|----|---:|---:|
| :a |  0 |  0 |
| :b |  1 |  1 |
| :b |  2 |  2 |
| :a |  3 |  3 |
| :c |  4 |  4 |
user> ds-b
_unnamed [5 3]:

| :a | :b | :c |
|----|---:|---:|
| :a |  0 |  6 |
| :b |  1 |  7 |
| :a |  2 |  8 |
| :b |  3 |  9 |
| :d |  4 | 10 |
user> (ds-join/pd-merge ds-a ds-b {:on [:a :b] :how :inner})
inner-join [2 4]:

| :a | :b | :c | :right.c |
|----|---:|---:|---------:|
| :a |  0 |  0 |        6 |
| :b |  1 |  1 |        7 |
user> (ds-join/pd-merge ds-a ds-b {:on [:a :b] :how :outer})
outer-join [8 4]:

| :a | :b | :c | :right.c |
|----|---:|---:|---------:|
| :a |  0 |  0 |        6 |
| :b |  1 |  1 |        7 |
| :b |  2 |  2 |          |
| :a |  3 |  3 |          |
| :c |  4 |  4 |          |
| :a |  2 |    |        8 |
| :b |  3 |    |        9 |
| :d |  4 |    |       10 |
```"
  ([left-ds right-ds options]
   (let [lhs-table-name (default-table-name left-ds "left")
         rhs-table-name (default-table-name right-ds "right")
         col-or-data->reader
         (fn [tuple-data ds]
           (if (and (sequential? tuple-data)
                    (not= 1 (count tuple-data)))
             (-> (ds-base/select-columns ds tuple-data)
                 (ds-readers/value-reader {:copying? true}))
             (if (sequential? tuple-data)
               (ds-base/column ds (first tuple-data))
               (ds-base/column ds tuple-data))))
         how (get options :how :inner)]
     (if (identical? how :cross)
       (do
         (errors/when-not-errorf
          (nil? (or (options :on)
                    (options :left-on)
                    (options :right-on)))
          "Columns must not be specified during a cross merge")
         (let [left-rc (ds-base/row-count left-ds)
               right-rc (ds-base/row-count right-ds)
               n-result (* left-rc right-rc)
               [lhs-indexes rhs-indexes]
               (if (< n-result Integer/MAX_VALUE)
                 [(dtype/make-reader :int32 n-result (quot idx right-rc))
                  (dtype/make-reader :int32 n-result (rem idx right-rc))]
                 [(dtype/make-reader :int64 n-result (quot idx right-rc))
                  (dtype/make-reader :int64 n-result (rem idx right-rc))])
               lhs-columns (map #(ds-col/select % lhs-indexes) (ds-base/columns left-ds))
               rhs-columns (map #(ds-col/select % rhs-indexes) (ds-base/columns right-ds))]
           (-> (ds-impl/new-dataset
                "cross-join"
                (nice-column-names
                 [lhs-table-name lhs-columns]
                 [rhs-table-name rhs-columns]))
               (update-join-metadata lhs-table-name rhs-table-name))))
       (let [left-on (get options :left-on (get options :on))
             right-on (get options :right-on (get options :on))
             left-on (when left-on (if-not (sequential? left-on) [left-on] left-on))
             right-on (when right-on (if-not (sequential? right-on) [right-on] right-on))
             on-int (->> (concat left-on right-on)
                         (filter (set/intersection (set left-on) (set right-on)))
                         (distinct)
                         (vec))
             _ (errors/when-not-errorf
                (== (count left-on) (count right-on))
                "Number of left join columns (%d) doesn't equal number of right join columns %d"
                (count left-on) (count right-on))
             left-join-data (col-or-data->reader left-on left-ds)
             right-join-data (col-or-data->reader right-on right-ds)


             {:keys [lhs-indexes rhs-indexes lhs-missing rhs-missing]}
             (hash-join-algo left-join-data right-join-data true true
                             (get options :operation-space :int32))
             ;;Ensure a reasonable column order

             left-cols (->> (concat left-on (ds-base/column-names left-ds)) distinct vec)
             right-cols (->> (concat right-on (ds-base/column-names right-ds)) distinct vec)
             left-ds (ds-base/select-columns left-ds left-cols)
             right-ds (ds-base/select-columns right-ds right-cols)]
         (case how
           :left
           (let [n-empty (count lhs-missing)
                 lhs-indexes (add-all! (dtype/clone lhs-indexes) lhs-missing)
                 lhs-cols (-> (ds-base/select-rows left-ds lhs-indexes)
                              (ds-base/columns))
                 rhs-cols (->> (ds-base/columns (ds-base/remove-columns right-ds on-int))
                               (map (fn [col]
                                      (ds-col/extend-column-with-empty
                                       (ds-col/select col rhs-indexes)
                                       n-empty))))]
             (-> (ds-impl/new-dataset
                  "left-outer-join"
                  (nice-column-names
                   [lhs-table-name lhs-cols]
                   [rhs-table-name rhs-cols]))
                 (update-join-metadata lhs-table-name rhs-table-name)))
           :right
           (let [n-empty (count rhs-missing)
                 rhs-indexes (add-all! (dtype/clone rhs-indexes) rhs-missing)
                 rhs-cols (-> (ds-base/select-rows right-ds rhs-indexes)
                              (ds-base/columns))
                 lhs-cols (->> (ds-base/columns (ds-base/remove-columns left-ds on-int))
                               (map (fn [col]
                                      (ds-col/extend-column-with-empty
                                       (ds-col/select col lhs-indexes)
                                       n-empty))))]
             (-> (ds-impl/new-dataset
                  "right-outer-join"
                  (nice-column-names
                   [rhs-table-name rhs-cols]
                   [lhs-table-name lhs-cols]))
                 (update-join-metadata lhs-table-name rhs-table-name)))
           :inner
           (let [lhs-cols (-> (ds-base/select-rows left-ds lhs-indexes)
                              (ds-base/columns))
                 rhs-cols (-> (ds-base/remove-columns right-ds on-int)
                              (ds-base/select-rows rhs-indexes)
                              (ds-base/columns))]
             (-> (ds-impl/new-dataset
                  "inner-join"
                  (nice-column-names
                   [lhs-table-name lhs-cols]
                   [rhs-table-name rhs-cols]))
                 (update-join-metadata lhs-table-name rhs-table-name)))
           :outer
           (let [n-left-empty (count rhs-missing)
                 n-right-empty (count lhs-missing)
                 ;;Order is intersection, left-missing, right-missing
                 lhs-indexes (add-all! (dtype/clone lhs-indexes) lhs-missing)
                 left-valid (ds-base/select-rows left-ds lhs-indexes)
                 right-valid (ds-base/select-rows right-ds rhs-indexes)
                 right-missing (ds-base/select-rows right-ds rhs-missing)
                 ;;For the columns we perhaps joined on
                 intersection-ds (-> (ds-base/select-columns left-valid on-int)
                                     (ds-base/concat-copying (ds-base/select-columns
                                                              right-missing on-int)))
                 left-full (-> (ds-base/remove-columns left-valid on-int)
                               (ds-base/extend-with-empty n-left-empty))
                 right-full (-> (ds-base/remove-columns right-valid on-int)
                                (ds-base/extend-with-empty n-right-empty)
                                (ds-base/concat-copying (ds-base/remove-columns
                                                         right-missing on-int)))]
             (-> (ds-impl/new-dataset
                  "outer-join"
                  (nice-column-names
                   [lhs-table-name (concat (ds-base/columns intersection-ds)
                                           (ds-base/columns left-full))]
                   [rhs-table-name (ds-base/columns right-full)]))
                 (update-join-metadata lhs-table-name rhs-table-name))))))))
  ([left-ds right-ds]
   (pd-merge left-ds right-ds {:on (set/intersection
                                    (set (ds-base/column-names left-ds))
                                    (set (ds-base/column-names right-ds)))})))


(defn- ensure-numeric-reader
  "Column is either a float or integer reader."
  ^Buffer [col]
  (let [col-dtype (casting/un-alias-datatype (dtype/get-datatype col))]
    (if (dtype-dt/datetime-datatype? col-dtype)
      (-> (dtype-dt/datetime->milliseconds col)
          (dtype/->reader))
      (dtype/->reader col (casting/simple-operation-space col-dtype)))))


(defn- asof-op->binary-pred
  ^BinaryPredicate [asof-op
                    ^Buffer lhs-rdr
                    ^Buffer rhs-rdr]
  (let [comp-op (argops/->binary-predicate asof-op)
        op-space (casting/simple-operation-space
                  (.elemwiseDatatype lhs-rdr)
                  (.elemwiseDatatype rhs-rdr))]
    (case op-space
      :int64
      (reify BinaryPredicate
        (binaryLong [this lhs-idx rhs-idx]
          (.binaryLong comp-op
                       (.readLong lhs-rdr lhs-idx)
                       (.readLong rhs-rdr rhs-idx))))
      :float64
      (reify BinaryPredicate
        (binaryLong [this lhs-idx rhs-idx]
          (.binaryDouble comp-op
                         (.readDouble lhs-rdr lhs-idx)
                         (.readDouble rhs-rdr rhs-idx)))))))


(defn- asof-lt
  [asof-op lhs rhs]
  (let [op-space (casting/simple-operation-space
                  (dtype/elemwise-datatype lhs)
                  (dtype/elemwise-datatype rhs))
        lhs-rdr (dtype/->reader lhs op-space)
        rhs-rdr (dtype/->reader rhs op-space)
        n-elems (.lsize lhs-rdr)
        n-right (.lsize rhs-rdr)
        retval (dtype/make-list :int32)
        comp-fn (asof-op->binary-pred asof-op lhs-rdr rhs-rdr)]
     (loop [lhs-idx 0
            rhs-idx 0]
        (when-not (or (== lhs-idx n-elems)
                      (== rhs-idx n-right))
          (let [found? (.binaryLong comp-fn lhs-idx rhs-idx)
                new-lhs-idx (if found?
                              (unchecked-inc lhs-idx)
                              lhs-idx)
                new-rhs-idx (if found?
                              rhs-idx
                              (unchecked-inc rhs-idx))]
            (when found?
              (.addLong retval (unchecked-int rhs-idx)))
            (recur new-lhs-idx new-rhs-idx))))
     [0 retval]))


(defn- asof-gt
  [asof-op lhs rhs]
  (let [op-space (casting/simple-operation-space
                  (dtype/elemwise-datatype lhs)
                  (dtype/elemwise-datatype rhs))
        lhs-rdr (dtype/->reader lhs op-space)
        rhs-rdr (dtype/->reader rhs op-space)
        n-elems (.lsize lhs-rdr)
        n-right (.lsize rhs-rdr)
        n-right-dec (dec n-right)
        retval (dtype/make-list :int32)
        comp-fn (asof-op->binary-pred asof-op lhs-rdr rhs-rdr)]
     (loop [lhs-idx (unchecked-dec n-elems)
            rhs-idx n-right-dec]
       (when-not (or (== lhs-idx -1)
                     (== rhs-idx -1))
         (let [found? (.binaryLong comp-fn lhs-idx rhs-idx)
               new-lhs-idx (if found?
                              (unchecked-dec lhs-idx)
                              lhs-idx)
               new-rhs-idx (if found?
                              rhs-idx
                              (unchecked-dec rhs-idx))]
           (when found?
             (.addLong retval (unchecked-int rhs-idx)))
           (recur new-lhs-idx new-rhs-idx))))
     ;;Now we have to reverse retval
     (let [rev-retval (dtype/make-list :int32)
           n-retval (.size retval)
           n-retval-dec (unchecked-dec n-retval)]
       (dotimes [iter n-retval]
         (.addLong rev-retval (.readLong retval (- n-retval-dec iter))))
       [(- n-elems (.size rev-retval))
        rev-retval])))

(def ^{:private true
       :tag BinaryOperator} abs-diff-op
  (reify BinaryOperators$DoubleBinaryOperator
    (binaryLong [this lhs rhs]
      (Math/abs (- rhs lhs)))
    (binaryDouble [this lhs rhs]
      (Math/abs (- rhs lhs)))))


(defn- abs-diff-bin-pred
  ^BinaryPredicate
  [^Buffer lhs-rdr
   ^Buffer rhs-rdr]
  (let [op-space (casting/simple-operation-space
                  (.elemwiseDatatype lhs-rdr)
                  (.elemwiseDatatype rhs-rdr))
        ^BinaryOperator comp-op abs-diff-op]
    (case op-space
      :int64
      (reify BinaryPredicate
        (binaryLong [this lhs-idx rhs-idx]
          (let [lhs-val (.readLong lhs-rdr lhs-idx)
                rhs-val (.readLong rhs-rdr rhs-idx)
                rhs-next-val (.readLong rhs-rdr (unchecked-inc rhs-idx))
                asof-diff (pmath/- (.binaryLong comp-op lhs-val rhs-val)
                                   (.binaryLong comp-op lhs-val rhs-next-val))]
            (< asof-diff 0))))
      :float64
      (reify BinaryPredicate
        (binaryLong [this lhs-idx rhs-idx]
          (let [lhs-val (.readLong lhs-rdr lhs-idx)
                rhs-val (.readLong rhs-rdr rhs-idx)
                rhs-next-val (.readLong rhs-rdr (unchecked-inc rhs-idx))
                asof-diff (pmath/- (.binaryDouble comp-op lhs-val rhs-val)
                                   (.binaryDouble comp-op lhs-val rhs-next-val))]
            (< asof-diff 0)))))))


(defn- asof-nearest
  [lhs rhs]
  (let [op-space (casting/simple-operation-space
                  (dtype/elemwise-datatype lhs)
                  (dtype/elemwise-datatype rhs))
        lhs-rdr (dtype/->reader lhs op-space)
        rhs-rdr (dtype/->reader rhs op-space)
        n-elems (.lsize lhs-rdr)
        n-right (.lsize rhs-rdr)
        n-right-dec (dec n-right)
        retval (dtype/make-list :int32)
        bin-pred (abs-diff-bin-pred lhs-rdr rhs-rdr)]
     (loop [lhs-idx 0
            rhs-idx 0]
       (when-not (or (== lhs-idx n-elems)
                     (== rhs-idx n-right-dec))
         (let [found? (.binaryLong bin-pred lhs-idx rhs-idx)
               new-lhs-idx (if found?
                              (unchecked-inc lhs-idx)
                              lhs-idx)
               new-rhs-idx (if found?
                              rhs-idx
                              (unchecked-inc rhs-idx))]
           (when found?
             (.add retval (unchecked-int rhs-idx)))
           (recur new-lhs-idx new-rhs-idx))))
     (dotimes [_iter (- n-elems (.size retval))]
       (.add retval n-right-dec))
     [0 retval]))


(defn left-join-asof
  "Perform a left join asof.  Similar to left join except this will join on nearest
  value.  lhs and rhs must be sorted by join-column.  join columns must be either
  datetime columns in which the join happens in millisecond space or they must be
  numeric - integer or floating point datatypes.

  Options:

  - `asof-op`- may be [:< :<= :nearest :>= :>] - type of join operation.  Defaults to
     <=."
  ([colname lhs rhs {:keys [asof-op]
                     :or {asof-op :<=}}]
   (when-not (#{:< :<= :nearest :>= :>} asof-op)
     (throw (Exception. (format "Unrecognized asof op: %s" asof-op))))
   (let [[lhs-colname rhs-colname] (colname->lhs-rhs-colnames colname)
         lhs-col (ds-base/column lhs lhs-colname)
         rhs-col (ds-base/column rhs rhs-colname)
         lhs-table-name (default-table-name lhs "left")
         rhs-table-name (default-table-name rhs "right")
         lhs-reader (ensure-numeric-reader lhs-col)
         rhs-reader (ensure-numeric-reader rhs-col)
         [rhs-offset rhs-indexes]
         (cond
           (#{:< :<=} asof-op)
           (asof-lt (keyword "tech.numerics" (name asof-op)) lhs-reader rhs-reader)
           (#{:> :>=} asof-op)
           (asof-gt (keyword "tech.numerics" (name asof-op)) lhs-reader rhs-reader)
           (= asof-op :nearest)
           (asof-nearest lhs-reader rhs-reader)
           :else
           (throw (Exception. "Unsupported")))
         rhs-offset (long rhs-offset)
         n-indexes (dtype/ecount rhs-indexes)
         n-empty (- (ds-base/row-count lhs) (+ rhs-offset n-indexes))
         lhs-columns (ds-base/columns lhs)
         rhs-columns
         (->> (ds-base/columns rhs)
              (map (fn [old-col]
                     (if (== 0 rhs-offset)
                       (ds-col/extend-column-with-empty
                        (ds-col/select old-col rhs-indexes)
                        n-empty)
                       (ds-col/prepend-column-with-empty
                        (ds-col/select old-col rhs-indexes)
                        rhs-offset)))))]
     (-> (ds-impl/new-dataset
          (format "asof-%s" (name asof-op))
          (nice-column-names
           [lhs-table-name lhs-columns]
           [rhs-table-name rhs-columns]))
         (update-join-metadata lhs-table-name rhs-table-name))))
  ([colname lhs rhs]
   (left-join-asof colname lhs rhs {})))
