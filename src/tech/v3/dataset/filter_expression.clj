(ns tech.v3.dataset.filter-expression
  "Filter expressions are the bare beginning of a query language specific
  to tmd processing.  They are used to transparently apply a given expression
  to indexed or non-indexed datasets."
  (:require [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.argops :as argops]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.binary-pred :as binary-pred]
            [tech.v3.datatype :as dtype]
            [tech.v3.dataset.column :as ds-col])
  (:import [java.util Comparator Objects]
           [tech.v3.datatype BinaryPredicate]))

(set! *warn-on-reflection* true)


(defn any-of
  [item-seq]
  #:tech.v3.dataset{:filter-type :any-of
                    :values (set item-seq)})


(defn filter-range
  ([start start-inclusive? stop stop-inclusive? comparator]
   (let [op-dtype (casting/simple-operation-space
                   (dtype/datatype start)
                   (dtype/datatype stop))
         ^Comparator actual-comparator (-> (argops/find-base-comparator
                                            comparator op-dtype)
                                           (argops/->comparator))
         comp (.compare actual-comparator start stop)]
     (errors/when-not-errorf
      (<= comp 0)
      "Start (%s) must be less than or equal to stop (%s)" start stop)
     #:tech.v3.dataset{:filter-type :range
                       :start start
                       :start-inclusive? start-inclusive?
                       :comparator comparator
                       :datatype op-dtype
                       :stop stop
                       :stop-inclusive? stop-inclusive?}))
  ([start start-inclusive? stop stop-inclusive?]
   (filter-range start start-inclusive? stop stop-inclusive? :tech.numerics/<))
  ([start stop]
   (filter-range start true stop false :tech.numerics/<)))


(defmulti ->predicate
  (fn [filter-exp column-dtype]
    (filter-exp :tech.v3.dataset/filter-type)))


(defmethod ->predicate :any-of
  [filter-exp column-dtype]
  (let [values (:tech.v3.dataset/values filter-exp)
        op-space (casting/simple-operation-space
                  column-dtype)]
    (case (count values)
      0 (constantly false)
      1 (let [value (first values)]
          (case op-space
            :int64 (let [value (long value)]
                     #(== value (unchecked-long %)))
            :float64 (let [^BinaryPredicate pred (binary-pred/builtin-ops :eq)
                           value (double value)]
                       #(.binaryDouble pred value (unchecked-double %)))
            (if (or (keyword? value)
                    (symbol? value))
              #(identical? value %)
              #(Objects/equals value %))))
      (if (casting/numeric-type? op-space)
        (->> values
             (map #(dtype/cast % column-dtype))
             (set))
        values))))


(defmethod ->predicate :range
  [{:tech.v3.dataset/keys [start start-inclusive? stop stop-inclusive?
                           comparator]}
   column-dtype]
  (let [comparator (-> (argops/find-base-comparator
                        comparator column-dtype)
                       (argops/->comparator))
        start (dtype/cast start column-dtype)
        stop (dtype/cast stop column-dtype)
        ^BinaryPredicate start-pred (if start-inclusive?
                                      (binary-pred/builtin-ops :tech.numerics/>=)
                                      (binary-pred/builtin-ops :tech.numerics/>))
        ^BinaryPredicate stop-pred (if stop-inclusive?
                                     (binary-pred/builtin-ops :tech.numerics/<=)
                                     (binary-pred/builtin-ops :tech.numerics/<))]
    #(when %
      (let [start-comp (.compare comparator % start)
            stop-comp (.compare comparator % stop)]
        (and (.binaryInt start-pred start-comp 0)
             (.binaryInt stop-pred stop-comp 0))))))


(defn filter-expression?
  [expr]
  (and (map? expr) (expr :tech.v3.dataset/filter-type)))


(defmulti apply-expression-ordered
  (fn [filter-exp column]
    (filter-exp :tech.v3.dataset/filter-type)))


(defn- default-apply-expression
  [filter-exp column]
  (-> (->predicate filter-exp (dtype/elemwise-datatype column))
      (argops/argfilter column)))


(defmethod apply-expression-ordered :default
  [filter-exp column]
  (default-apply-expression filter-exp column))


(defmethod apply-expression-ordered :range
  [filter-exp column]
  (if (and (identical? :tech.numerics/< (:tech.v3.dataset/comparator filter-exp))
           (.isEmpty (ds-col/missing column)))
    (let [order (get-in (meta column) [:statistics :order])
          {:tech.v3.dataset/keys
           [start start-inclusive? stop stop-inclusive?]} filter-exp
          start-idx (argops/binary-search column start {:comparator order})
          stop-idx (argops/binary-search column stop {:comparator order})
          n-elems (dtype/ecount column)
          start-idx (if-not start-inclusive?
                      (loop [start-idx start-idx]
                        (if (and (< start-idx stop-idx)
                                 (Objects/equals start (column start-idx)))
                          (recur (unchecked-inc start-idx))
                          start-idx))
                      start-idx)
          stop-idx (if-not stop-inclusive?
                     (loop [stop-idx stop-idx]
                       (if (and (> stop-idx start-idx)
                                (Objects/equals stop (column stop-idx)))
                         (recur (unchecked-dec stop-idx))
                         stop-idx))
                     (loop [stop-idx stop-idx]
                       (if (and (< stop-idx n-elems)
                                (Objects/equals stop (column stop-idx)))
                         (recur (unchecked-inc stop-idx))
                         (unchecked-dec stop-idx))))]
      (range start-idx (inc stop-idx)))
    (default-apply-expression filter-exp column)))


(defn apply-filter-expression
  "Apply a filter expression returning a reader of indexes."
  [filter-exp column]
  (let [colstats (get (meta column) :statistics)]
    (cond
      ;;TODO - index check along with another multimethod goes here
      (or (identical? :tech.numerics/> (:order colstats))
          (identical? :tech.numerics/< (:order colstats)))
      (apply-expression-ordered filter-exp column)
      :else
      (default-apply-expression filter-exp column))))
