(ns tech.ml.dataset.etl.pipeline-operators
  (:require [tech.ml.dataset :as ds]
            [tech.ml.dataset.column :as ds-col]
            [tech.ml.protocols.etl :as etl-proto]
            [tech.datatype :as dtype]
            [tech.ml.dataset.etl.column-filters :as column-filters]
            [tech.ml.dataset.etl.defaults :refer [etl-datatype]]
            [tech.ml.dataset.etl.math-ops :as math-ops]
            [tech.ml.utils :as utils]
            [tech.compute.tensor :as ct]
            [tech.compute.tensor.operations :as ct-ops])
  (:refer-clojure :exclude [remove])
  (:import [tech.ml.protocols.etl
            PETLSingleColumnOperator
            PETLMultipleColumnOperator]))


(defonce ^:dynamic *etl-operator-registry* (atom {}))


(defn register-etl-operator!
  [op-kwd op]
  (-> (swap! *etl-operator-registry* assoc op-kwd op)
      keys))


(defn get-etl-operator
  [op-kwd]
  (if-let [retval (get @*etl-operator-registry* op-kwd)]
    retval
    (throw (ex-info (format "Failed to find etl operator %s" op-kwd)
                    {:op-keyword op-kwd}))))


(defn apply-pipeline-operator
  [{:keys [pipeline dataset options]} op]
  (let [inference? (:inference? options)
        recorded? (or (:recorded? options) inference?)
        [op context] (if-not recorded?
                       [op {}]
                       [(:operation op) (:context op)])
        op-type (keyword (name (first op)))
        col-selector (second op)
        op-args (drop 2 op)
        col-seq (column-filters/select-columns dataset col-selector)
        op-impl (get-etl-operator op-type)
        [context options] (if-not recorded?
                            (let [context
                                  (etl-proto/build-etl-context-columns
                                   op-impl dataset col-seq op-args)]
                              [context (if (:label-map context)
                                         (update options
                                                 :label-map
                                                 merge
                                                 (:label-map context))
                                         options)])
                            [context options])
        dataset (etl-proto/perform-etl-columns
                 op-impl dataset col-seq op-args context)]
    {:dataset dataset
     :options options
     :pipeline (conj pipeline {:operation (->> (concat [(first op) col-seq]
                                                       (drop 2 op))
                                               vec)
                               :context context})}))



(defmacro def-etl-operator
  [op-symbol op-context-code op-code]
  `(do (register-etl-operator! ~(keyword (name op-symbol))
                               (reify PETLSingleColumnOperator
                                 (build-etl-context [~'op ~'dataset ~'column-name ~'op-args]
                                   ~op-context-code)
                                 (perform-etl [~'op ~'dataset ~'column-name ~'op-args ~'context]
                                   ~op-code)))
       (defn ~op-symbol
         [dataset# col-selector# & op-args#]
         (-> (apply-pipeline-operator
              {:pipeline []
               :options {}
               :dataset dataset#}
              (-> (concat '[~op-symbol]
                          [col-selector#]
                          op-args#)
                  vec))
             :dataset))))


(def-etl-operator
  set-attribute
  nil
  (let [retval (ds/update-column
                dataset column-name
                (fn [col]
                  (->> (merge (ds-col/metadata col)
                              (apply hash-map op-args))
                       (ds-col/set-metadata col))))]
    retval))


(def-etl-operator
  remove
  nil
  (ds/remove-column dataset column-name))


(def-etl-operator
  replace-missing
  (let [missing-val (first op-args)]
    {:missing-value missing-val})
  (ds/update-column
   dataset column-name
   (fn [col]
     (let [missing-indexes (ds-col/missing col)]
       (let [retval
             (ds-col/set-values col (map vector
                                         (seq missing-indexes)
                                         (repeat (:missing-value context))))]
         retval)))))


(defn- make-string-table-from-args
  [op-args]
  (let [used-indexes (->> op-args
                          (map (fn [item]
                                 (if (string? item)
                                   nil
                                   (second item))))
                          (clojure.core/remove nil?)
                          set)]
    (->> op-args
         (reduce (fn [[lookup-table used-indexes] item]
                   (if (string? item)
                     (let [next-idx (first (clojure.core/remove used-indexes (range)))]
                       [(assoc lookup-table item next-idx)
                        (conj used-indexes next-idx)])
                     (let [[item-name item-idx] item]
                       [(assoc lookup-table item-name item-idx)
                        (conj used-indexes item-idx)])))
                 [{} used-indexes])
         first)))



(register-etl-operator!
 :string->number
 (reify PETLMultipleColumnOperator
   (build-etl-context-columns [op dataset column-name-seq op-args]
     ;;Label maps are special and used outside of this context do we have
     ;;treat them separately
     (let [provided-table (when-let [table-vals (seq (first op-args))]
                            (make-string-table-from-args table-vals))]
       {:label-map (->> column-name-seq
                        (map (fn [column-name]
                               [column-name (if provided-table
                                              provided-table
                                              (make-string-table-from-args (ds-col/unique
                                                                            (ds/column
                                                                             dataset column-name))))]))
                        (into {}))}))

   (perform-etl-columns [op dataset column-name-seq op-args context]
     (->> column-name-seq
          (reduce (fn [dataset column-name]
                    (ds/update-column
                     dataset column-name
                     (fn [col]
                       (let [existing-values (ds-col/column-values col)
                             str-table (get-in context [:label-map column-name])
                             new-col-dtype (etl-datatype)
                             data-values (dtype/make-array-of-type
                                          new-col-dtype
                                          (->> existing-values
                                               (map (fn [item-val]
                                                      (if-let [lookup-val (get str-table item-val)]
                                                        lookup-val
                                                        (throw (ex-info (format "Failed to find lookup for value %s"
                                                                                item-val)
                                                                        {:item-value item-val
                                                                         :possible-values (set (keys str-table))
                                                                         :column-name column-name}))))))
                                          {:unchecked? true})
                             retval (ds-col/new-column col new-col-dtype data-values)]
                         retval))))
                  dataset)))))


(def-etl-operator
  replace-string
  nil
  (ds/update-column
   dataset column-name
   (fn [col]
     (let [existing-values (ds-col/column-values col)
           [src-str replace-str] op-args
           data-values (into-array String (->> existing-values
                                            (map (fn [str-value]
                                                   (if (= str-value src-str)
                                                     replace-str
                                                     str-value)))))]
       (ds-col/new-column col :string data-values)))))


(def-etl-operator
  ->etl-datatype
  nil
  (ds/update-column
   dataset column-name
   (fn [col]
     (if-not (= (dtype/get-datatype col) (etl-datatype))
       (let [new-col-dtype (etl-datatype)
             col-values (ds-col/column-values col)
             data-values (dtype/make-array-of-type
                          new-col-dtype
                          (if (= :boolean (dtype/get-datatype col))
                            (map #(if % 1 0) col-values)
                            col-values))]
         (ds-col/new-column col new-col-dtype data-values))
       col))))


(defn finalize-math-result
  [result dataset column-name]
  (-> (if-let [src-col (ds/maybe-column dataset column-name)]
        (ds-col/set-metadata result (dissoc (ds-col/metadata src-col)
                                            :categorical?))
        (ds-col/set-metadata result (dissoc (ds-col/metadata result)
                                            :categorical?
                                            :target?)))
      (ds-col/set-name column-name)))


(defn operator-eval-expr
  [dataset column-name math-expr]
  (ds/add-or-update-column
   dataset
   (-> (math-ops/eval-expr {:dataset dataset
                            :column-name column-name}
                           math-expr)
       (finalize-math-result dataset column-name))))


(def-etl-operator
  m=
  nil
  (operator-eval-expr dataset column-name (first op-args)))


(defn- ->row-broadcast
  [item-tensor]
  (if (number? item-tensor)
    item-tensor
    (ct/in-place-reshape item-tensor [(ct/ecount item-tensor) 1])))


(defn- sub-divide-bias
  "Perform the operation:
  (-> col
      (- sub-val)
      (* mult-val)
      (+ bias-val))."
  [dataset column-name-seq sub-val divide-val bias-val]
  (let [src-data (ds/select dataset column-name-seq :all)
        [src-rows src-cols] (ct/shape src-data)
        colseq (ds/columns src-data)
        etl-dtype (etl-datatype)
        ;;Storing data column-major so the row is incremention fast.
        backing-store (ct/new-tensor [src-cols src-rows]
                                     :datatype etl-dtype
                                     :init-value nil)
        _ (dtype/copy-raw->item! (->> colseq
                                      (map (fn [col]
                                             (when-not (= (int src-rows) (ct/ecount col))
                                               (throw (ex-info "Column is wrong size; ragged tables not supported." {})))
                                             (ds-col/column-values col))))
                                 (ct/tensor->buffer backing-store)
                                 0 {:unchecked? true})

        backing-store (-> backing-store
                          (ct-ops/- (->row-broadcast sub-val))
                          (ct-ops// (->row-broadcast divide-val))
                          (ct-ops/+ (->row-broadcast bias-val)))]

    ;;apply result back to main table
    (->> colseq
         (map-indexed vector)
         (reduce (fn [dataset [col-idx col]]
                   (ds/update-column
                    dataset (ds-col/column-name col)
                    (fn [incoming-col]
                      (ds-col/new-column incoming-col etl-dtype
                                         (ct/select backing-store col-idx :all)
                                         (dissoc (ds-col/metadata incoming-col)
                                                 :categorical?)))))
                 dataset))))


(register-etl-operator!
 :range-scaler
  (reify PETLMultipleColumnOperator
    (build-etl-context-columns [op dataset column-name-seq op-args]
      (->> column-name-seq
           (map (fn [column-name]
                  [column-name
                   (-> (ds/column dataset column-name)
                       (ds-col/stats [:min :max]))]))
           (into {})))

    (perform-etl-columns [op dataset column-name-seq op-args context]
      (if-let [column-name-seq (->> column-name-seq
                                    (clojure.core/remove #(= (get-in context [% :min])
                                                             (get-in context [% :max])))
                                    seq)]
        (let [colseq (map (partial dataset ds/column) column-name-seq)
              etl-dtype (etl-datatype)
              context-map-seq (map #(get context %) column-name-seq)
              min-values (ct/->tensor (mapv :min context-map-seq) :datatype etl-dtype)
              max-values (ct/->tensor (mapv :max context-map-seq) :datatype etl-dtype)
              col-ranges (ct/binary-op! (ct/clone min-values) 1.0 max-values 1.0 min-values :-)
              [range-min range-max] (if (seq op-args)
                                      (first op-args)
                                      [-1 1])
              range-min (double range-min)
              range-max (double range-max)
              target-range (- range-max
                              range-min)
              divisor (ct/binary-op! (ct/clone col-ranges) 1.0 col-ranges 1.0 target-range :/)]
          (sub-divide-bias dataset column-name-seq min-values divisor range-min))
        ;;No columns, noop.
        dataset))))


(defn- bool-arg
  [argmap argname default-value]
  (if (contains? argmap argname)
    (boolean (get argmap argname))
    (boolean default-value)))


(register-etl-operator!
 :std-scaler
 (reify PETLMultipleColumnOperator
   (build-etl-context-columns [op dataset column-name-seq op-args]
     (let [argmap (apply hash-map op-args)
           with-mean? (bool-arg argmap :with-mean? true)
           with-std? (bool-arg argmap :with-std? true)
           stats-seq (-> (concat (when with-mean? [:mean])
                                 (when with-std? [:standard-deviation])))]
       (->> column-name-seq
            (map (fn [column-name]
                   [column-name
                    (-> (ds/column dataset column-name)
                        (ds-col/stats stats-seq))]))
            (into {}))))

   (perform-etl-columns [op dataset column-name-seq op-args context]
     ;;Avoid divide by zero.
     (if-let [column-name-seq (->> column-name-seq
                                   (clojure.core/remove
                                    #(= 0 (get-in context [% :standard-deviation]))))]
       (let [first-ctx (get context (first column-name-seq))
             colseq (map (partial ds/column dataset) column-name-seq)
             use-mean? (contains? first-ctx :mean)
             use-std? (contains? first-ctx :standard-deviation)
             etl-dtype (etl-datatype)
             context-map-seq (map #(get context (ds-col/column-name %)) colseq)
             mean-values (if use-mean?
                           (ct/->tensor (mapv :mean context-map-seq) :datatype etl-dtype)
                           0)
             std-values (if use-std?
                          (ct/->tensor (mapv :standard-deviation context-map-seq) :datatype etl-dtype)
                          1.0)]
         (sub-divide-bias dataset column-name-seq mean-values std-values 0.0))
       ;;no columns, noop
       dataset))))
