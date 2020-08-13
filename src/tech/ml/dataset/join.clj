(ns ^:no-doc tech.ml.dataset.join
  "implementation of join algorithms, both exact (hash-join) and near."
  (:require [tech.v2.datatype :as dtype]
            [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.builtin-op-providers
             :refer [dtype->storage-constructor]
             :as builtin-op-providers]
            [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.functional :as dfn]
            [tech.v2.datatype.datetime :as dtype-dt]
            [tech.v2.datatype.datetime.operations :as dtype-dt-ops]
            [tech.parallel.for :as parallel-for]
            [tech.ml.dataset.column :as ds-col]
            [tech.ml.dataset.base :as ds-base]
            [primitive-math :as pmath])
  (:import [tech.v2.datatype ObjectReader]
           [tech.v2.datatype BooleanOp$DoubleBinary BooleanOp$LongBinary
            BinaryOperators$DoubleBinary BinaryOperators$LongBinary]
           [java.util List HashSet]
           [it.unimi.dsi.fastutil.longs LongArrayList]
           [it.unimi.dsi.fastutil.ints IntArrayList IntList]))


(set! *warn-on-reflection* true)


(defmacro datatype->group-by
  [datatype]
  (case datatype
    :int32 `dfn/arggroup-by-stable-int
    :int64 `dfn/arggroup-by-stable))

(defn- colname->str
  ^String [item]
  (cond
    (string? item) item
    (keyword? item) (name item)
    (symbol? item) (name item)
    :else (str item)))


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


(defmacro hash-join-impl
  [datatype colname lhs rhs options]
  `(let [[lhs-colname# rhs-colname#] (colname->lhs-rhs-colnames ~colname)
         lhs# ~lhs
         rhs# ~rhs
         options# ~options
         lhs-col# (lhs# lhs-colname#)
         rhs-col# (rhs# rhs-colname#)
         lhs-missing?# (:lhs-missing? options#)
         rhs-missing?# (:rhs-missing? options#)
         lhs-dtype# (dtype/get-datatype lhs-col#)
         rhs-dtype# (dtype/get-datatype rhs-col#)
         op-dtype# (cond
                     (= lhs-dtype# rhs-dtype#)
                     rhs-dtype#
                     (and (casting/numeric-type? lhs-dtype#)
                          (casting/numeric-type? rhs-dtype#))
                     (builtin-op-providers/widest-datatype lhs-dtype# rhs-dtype#)
                     :else
                     :object)
         idx-groups# ((datatype->group-by ~datatype)
                      identity (dtype/->reader lhs-col# op-dtype#))
         ^List rhs-col# (dtype/->reader rhs-col# op-dtype#)
         n-elems# (dtype/ecount rhs-col#)
         {lhs-indexes# :lhs-indexes
          rhs-indexes# :rhs-indexes
          rhs-missing# :rhs-missing
          lhs-found# :lhs-found}
         (parallel-for/indexed-map-reduce
          n-elems#
          (fn [^long outer-idx# ^long n-indexes#]
            (let [lhs-indexes# (dtype->storage-constructor ~datatype)
                  rhs-indexes# (dtype->storage-constructor ~datatype)
                  rhs-missing# (dtype->storage-constructor ~datatype)
                  lhs-found# (HashSet.)]
              (dotimes [inner-idx# n-indexes#]
                (let [idx# (+ outer-idx# inner-idx#)
                      rhs-val# (.get rhs-col# idx#)]
                  (if-let [item# (typecast/datatype->list-cast-fn
                                  ~datatype
                                  (get idx-groups# rhs-val#))]
                    (do
                      (when lhs-missing?# (.add lhs-found# rhs-val#))

                      (dotimes [n-iters# (.size item#)] (.add rhs-indexes# idx#))
                      (.addAll lhs-indexes# item#))
                    (when rhs-missing?# (.add rhs-missing# idx#)))))
              {:lhs-indexes lhs-indexes#
               :rhs-indexes rhs-indexes#
               :rhs-missing rhs-missing#
               :lhs-found lhs-found#}))
          (partial reduce (fn [accum# nextmap#]
                            (->> accum#
                                 (map (fn [[k# v#]]
                                        (if (= k# :lhs-found)
                                          (.addAll ^HashSet v#
                                                   ^HashSet (nextmap# k#))
                                          (.addAll
                                           (typecast/datatype->list-cast-fn
                                            ~datatype v#)
                                           (typecast/datatype->list-cast-fn
                                            ~datatype (nextmap# k#))))
                                        [k# v#]))
                                 (into {})))))
         lhs-missing# (when lhs-missing?#
                        (reduce (fn [lhs-missing# lhs-missing-key#]
                                  (.addAll (typecast/datatype->list-cast-fn
                                            ~datatype lhs-missing#)
                                           (typecast/datatype->list-cast-fn
                                            ~datatype (get idx-groups#
                                                           lhs-missing-key#)))
                                 lhs-missing#)
                               (dtype->storage-constructor ~datatype)
                               (->> (keys idx-groups#)
                                    (remove #(.contains ^HashSet lhs-found# %)))))]
     (finalize-join-result lhs-colname# rhs-colname# lhs# rhs#
                           lhs-indexes# rhs-indexes#
                           lhs-missing#
                           rhs-missing#)))

(defn- typed-add-all!
  [target new-data]
  (case (dtype/get-datatype target)
    :int32 (.addAll (typecast/datatype->list-cast-fn :int32 target)
                    (typecast/datatype->list-cast-fn :int32 new-data))
    :int64 (.addAll (typecast/datatype->list-cast-fn :int64 target)
                    (typecast/datatype->list-cast-fn :int64 new-data)))
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
         (-> (ds-base/from-prototype
              lhs "inner-join"
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
               rhs-indexes (typed-add-all! (dtype/clone rhs-indexes) rhs-missing)
               rhs-columns (map #(ds-col/select % rhs-indexes) rhs-columns)
               lhs-columns
               (->> (concat [lhs-join-column] lhs-columns)
                    (map (fn [old-col]
                           (ds-col/extend-column-with-empty
                            (ds-col/select old-col lhs-indexes)
                            n-empty))))]
           (-> (ds-base/from-prototype
                lhs "right-outer-join"
                (nice-column-names
                 [lhs-table-name lhs-columns]
                 [rhs-table-name (concat [(ds-col/select rhs-join-column rhs-indexes)]
                                         rhs-columns)]))
               (update-join-metadata lhs-table-name rhs-table-name)))})
      (when lhs-missing
        {:lhs-missing lhs-missing
         :left-outer
         (let [n-empty (count lhs-missing)
               lhs-indexes (typed-add-all! (dtype/clone lhs-indexes) lhs-missing)
               lhs-columns (map #(ds-col/select % lhs-indexes) lhs-columns)
               rhs-columns
               (->> (concat [rhs-join-column] rhs-columns)
                    (map (fn [old-col]
                           (ds-col/extend-column-with-empty
                            (ds-col/select old-col rhs-indexes)
                            n-empty))))]
           (-> (ds-base/from-prototype
                lhs "left-outer-join"
                (nice-column-names
                 [lhs-table-name (concat [(ds-col/select lhs-join-column lhs-indexes)]
                                         lhs-columns)]
                 [rhs-table-name rhs-columns]))
               (update-join-metadata lhs-table-name rhs-table-name)))}))))


(defn hash-join-int32
  [colname lhs rhs options]
  (hash-join-impl :int32 colname lhs rhs options))


(defn hash-join-int64
  [colname lhs rhs options]
  (hash-join-impl :int64 colname lhs rhs options))


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
   (case operation-space
     :int32 (hash-join-int32 colname lhs rhs options)
     :int64 (hash-join-int64 colname lhs rhs options))))


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


(defn- ensure-numeric-reader
  "Column is either a float or integer reader."
  [col]
  (let [col-dtype (casting/un-alias-datatype (dtype/get-datatype col))]
    (if (dtype-dt/datetime-datatype? col-dtype)
      (if (= :duration col-dtype)
        (-> (dtype-dt-ops/get-milliseconds col)
            (dtype/->reader))
        (-> (dtype-dt-ops/get-epoch-milliseconds col)
            (dtype/->reader)))
      (cond
        (casting/float-type? col-dtype)
        (dtype/->reader col :float64)
        (casting/integer-type? col-dtype)
        (dtype/->reader col :int64)
        :else
        (throw (Exception. (format "Datatype is not a numeric type: %s"
                                   col-dtype)))))))


(defn- make-float64-comp-op
  ^BooleanOp$DoubleBinary [op-kwd]
  (case op-kwd
    :< (reify BooleanOp$DoubleBinary
         (op [this lhs rhs]
           (pmath/< lhs rhs)))
    :<= (reify BooleanOp$DoubleBinary
         (op [this lhs rhs]
           (pmath/<= lhs rhs)))
    :>= (reify BooleanOp$DoubleBinary
          (op [this lhs rhs]
            (pmath/>= lhs rhs)))
    :> (reify BooleanOp$DoubleBinary
         (op [this lhs rhs]
           (pmath/> lhs rhs)))))


(defn- make-int64-comp-op
  ^BooleanOp$LongBinary [op-kwd]
  (case op-kwd
    :< (reify BooleanOp$LongBinary
         (op [this lhs rhs]
           (pmath/< lhs rhs)))
    :<= (reify BooleanOp$LongBinary
         (op [this lhs rhs]
           (pmath/<= lhs rhs)))
    :>= (reify BooleanOp$LongBinary
          (op [this lhs rhs]
            (pmath/>= lhs rhs)))
    :> (reify BooleanOp$LongBinary
         (op [this lhs rhs]
           (pmath/> lhs rhs)))))


(defn- make-float64-diff-op
  ^BinaryOperators$DoubleBinary []
  (reify BinaryOperators$DoubleBinary
    (op [this lhs rhs]
      (Math/abs (- rhs lhs)))))


(defn- make-int64-diff-op
  ^BinaryOperators$LongBinary []
  (reify BinaryOperators$LongBinary
    (op [this lhs rhs]
      (Math/abs (- rhs lhs)))))


(defmacro asof-lt
  [datatype asof-op lhs rhs]
  `(let [lhs-rdr# (typecast/datatype->reader ~datatype ~lhs)
         rhs-rdr# (typecast/datatype->reader ~datatype ~rhs)
         comp-op# ~(case datatype
                     :int64 `(make-int64-comp-op ~asof-op)
                     :float64 `(make-float64-comp-op ~asof-op))
         n-elems# (.lsize lhs-rdr#)
         n-right# (.lsize rhs-rdr#)
         n-right-dec# (dec n-right#)
         retval# (IntArrayList. n-elems#)]
     (loop [lhs-idx# 0
            rhs-idx# 0]
       (when-not (or (== lhs-idx# n-elems#)
                     (== rhs-idx# n-right#))
         (let [lhs-val# (.read lhs-rdr# lhs-idx#)
               rhs-val# (.read rhs-rdr# rhs-idx#)
               found?# (.op comp-op# lhs-val# rhs-val#)
               new-lhs-idx# (if found?#
                              (unchecked-inc lhs-idx#)
                              lhs-idx#)
               new-rhs-idx# (if found?#
                              rhs-idx#
                              (unchecked-inc rhs-idx#))]
           (when found?#
             (.add retval# (unchecked-int rhs-idx#)))
           (recur new-lhs-idx# new-rhs-idx#))))
     [0 retval#]))


(defmacro asof-gt
  [datatype asof-op lhs rhs]
  `(let [lhs-rdr# (typecast/datatype->reader ~datatype ~lhs)
         rhs-rdr# (typecast/datatype->reader ~datatype ~rhs)
         comp-op# ~(case datatype
                     :int64 `(make-int64-comp-op ~asof-op)
                     :float64 `(make-float64-comp-op ~asof-op))
         n-elems# (.lsize lhs-rdr#)
         n-right# (.lsize rhs-rdr#)
         n-right-dec# (dec n-right#)
         retval# (IntArrayList. n-elems#)]
     (loop [lhs-idx# (unchecked-dec n-elems#)
            rhs-idx# n-right-dec#]
       (when-not (or (== lhs-idx# -1)
                     (== rhs-idx# -1))
         (let [lhs-val# (.read lhs-rdr# lhs-idx#)
               rhs-val# (.read rhs-rdr# rhs-idx#)
               found?# (.op comp-op# lhs-val# rhs-val#)
               new-lhs-idx# (if found?#
                              (unchecked-dec lhs-idx#)
                              lhs-idx#)
               new-rhs-idx# (if found?#
                              rhs-idx#
                              (unchecked-dec rhs-idx#))]
           (when found?#
             (.add retval# (unchecked-int rhs-idx#)))
           (recur new-lhs-idx# new-rhs-idx#))))
     ;;Now we have to reverse retval#
     (let [rev-retval# (IntArrayList. n-elems#)
           n-retval# (.size retval#)
           n-retval-dec# (unchecked-dec n-retval#)]
       (dotimes [iter# n-retval#]
         (.add rev-retval# (.get retval# (- n-retval-dec# iter#))))
       [(- n-elems# (.size rev-retval#))
        rev-retval#])))


(defmacro asof-nearest
  [datatype lhs rhs]
  `(let [lhs-rdr# (typecast/datatype->reader ~datatype ~lhs)
         rhs-rdr# (typecast/datatype->reader ~datatype ~rhs)
         comp-op# ~(case datatype
                     :int64 `(make-int64-diff-op)
                     :float64 `(make-float64-diff-op))
         n-elems# (.lsize lhs-rdr#)
         n-right# (.lsize rhs-rdr#)
         n-right-dec# (dec n-right#)
         retval# (IntArrayList. n-elems#)]
     (loop [lhs-idx# 0
            rhs-idx# 0]
       (when-not (or (== lhs-idx# n-elems#)
                     (== rhs-idx# n-right-dec#))
         (let [lhs-val# (.read lhs-rdr# lhs-idx#)
               rhs-val# (.read rhs-rdr# rhs-idx#)
               rhs-next-val# (.read rhs-rdr# (unchecked-inc rhs-idx#))
               asof-diff# (pmath/- (.op comp-op# lhs-val# rhs-val#)
                                   (.op comp-op# lhs-val# rhs-next-val#))
               found?# (<= asof-diff# 0)
               new-lhs-idx# (if found?#
                              (unchecked-inc lhs-idx#)
                              lhs-idx#)
               new-rhs-idx# (if found?#
                              rhs-idx#
                              (unchecked-inc rhs-idx#))]
           (when found?#
             (.add retval# (unchecked-int rhs-idx#)))
           (recur new-lhs-idx# new-rhs-idx#))))
     (dotimes [iter# (- n-elems# (.size retval#))]
       (.add retval# n-right-dec#))
     [0 retval#]))


(defn left-join-asof
  "Perform a left join asof.  Similar to left join except this will join on nearest
  value.  lhs and rhs must be sorted by join-column.
  join columns must be either datetime columns in which
  the join happens in millisecond space or they must be numeric - integer or floating
  point datatypes.

  options:
  - `asof-op`- may be [:< :<= :nearest :>= :>] - type of join operation.  Defaults to
     <=."
  ([colname lhs rhs {:keys [asof-op]
                      :or {asof-op :<=}
                     :as options}]
   (when-not (#{:< :<= :nearest :>= :>} asof-op)
     (throw (Exception. (format "Unrecognized asof op: %s" asof-op))))
   (let [[lhs-colname rhs-colname] (colname->lhs-rhs-colnames colname)
         lhs-col (lhs lhs-colname)
         rhs-col (rhs rhs-colname)
         lhs-table-name (default-table-name lhs "left")
         rhs-table-name (default-table-name rhs "right")
         lhs-reader (ensure-numeric-reader lhs-col)
         rhs-reader (ensure-numeric-reader rhs-col)
         op-dtype (builtin-op-providers/widest-datatype
                   (dtype/get-datatype lhs-reader)
                   (dtype/get-datatype rhs-reader))
         [rhs-offset rhs-indexes]
         (case op-dtype
           :float64 (cond
                      (#{:< :<=} asof-op)
                      (asof-lt :float64 asof-op lhs-reader rhs-reader)
                      (#{:> :>=} asof-op)
                      (asof-gt :float64 asof-op lhs-reader rhs-reader)
                      (= asof-op :nearest)
                      (asof-nearest :float64 lhs-reader rhs-reader)
                      :else
                      (throw (Exception. "Unsupported")))
           :int64 (cond
                    (#{:< :<=} asof-op)
                    (asof-lt :int64 asof-op lhs-reader rhs-reader)
                    (#{:> :>=} asof-op)
                    (asof-gt :int64 asof-op lhs-reader rhs-reader)
                    (= asof-op :nearest)
                    (asof-nearest :int64 lhs-reader rhs-reader)
                    :else
                    (throw (Exception. "Unsupported"))))
         rhs-offset (long rhs-offset)
         n-indexes (dtype/ecount rhs-indexes)
         n-empty (- (ds-base/row-count lhs) (+ rhs-offset n-indexes))]
     (let [lhs-columns (ds-base/columns lhs)
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
       (-> (ds-base/from-prototype
            lhs (format "asof-%s" (name asof-op))
            (nice-column-names
             [lhs-table-name lhs-columns]
             [rhs-table-name rhs-columns]))
           (update-join-metadata lhs-table-name rhs-table-name)))))
  ([colname lhs rhs]
   (left-join-asof colname lhs rhs {})))
