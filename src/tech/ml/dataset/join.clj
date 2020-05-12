(ns tech.ml.dataset.join
  (:require [tech.v2.datatype :as dtype]
            [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.builtin-op-providers
             :refer [dtype->storage-constructor]
             :as builtin-op-providers]
            [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.functional :as dfn]
            [tech.parallel.for :as parallel-for]
            [tech.ml.dataset.column :as ds-col]
            [tech.ml.dataset.base :as ds-base])
  (:import [tech.v2.datatype ObjectReader]
           [tech.ml.dataset.impl.column Column]
           [java.util List HashSet]
           [it.unimi.dsi.fastutil.longs LongArrayList]))


(defmacro datatype->group-by
  [datatype]
  (case datatype
    :int32 `dfn/arggroup-by-int
    :int64 `dfn/arggroup-by))

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
    (keyword new-name)))


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
  (let [name-data (->> (map meta result-table)
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
