(ns tech.ml.dataset.pipeline.column-filters
  (:require [tech.v2.datatype :as dtype]
            [tech.v2.datatype.casting :as casting]
            [tech.ml.dataset.base :as ds]
            [tech.ml.dataset.column :as ds-col]
            [tech.ml.dataset.pipeline.base :as pipe-base]
            [clojure.set :as c-set])
  (:refer-clojure :exclude [string? not and or boolean?
                            > >= < <=]))


(defn select-column-names
  [dataset column-name-seq]
  (cond
    (fn? column-name-seq)
    (column-name-seq dataset)
    (clojure.core/or (= :all column-name-seq)
                     (sequential? column-name-seq)
                     (nil? column-name-seq))
    (->> (ds/select-columns dataset (clojure.core/or column-name-seq :all))
         ds/column-names)
    (clojure.core/or (clojure.core/string? column-name-seq)
                     (keyword? column-name-seq))
    [column-name-seq]
    :else
    (throw (ex-info (format "Unrecognized argument to select columns: %s"
                            column-name-seq)
                    {}))))


(defn select-columns
  [dataset column-name-seq]
  (->> (select-column-names
        dataset
        column-name-seq)
       (map (partial ds/column dataset))))


(defn column-filter
  [dataset col-filter-fn & [column-name-seq]]
  (->> (select-columns dataset column-name-seq)
       (filter col-filter-fn)
       (map ds-col/column-name)))


(defn of-datatype?
  "Return column-names  of a given datatype."
  [dataset datatype & [column-name-seq]]
  (column-filter dataset #(do
                            (= datatype (dtype/get-datatype %)))
                 column-name-seq))


(defn string?
  [dataset & [column-name-seq]]
  (of-datatype? dataset :string column-name-seq))


(defn boolean?
  [dataset & [column-name-seq]]
  (of-datatype? dataset :boolean column-name-seq))


(defn numeric?
  [dataset & [column-name-seq]]
  (column-filter dataset #(casting/numeric-type? (dtype/get-datatype %))
                 column-name-seq))


(defn categorical?
  [dataset & [column-name-seq]]
  (column-filter dataset (comp :categorical? ds-col/metadata)
                 column-name-seq))


(defn missing?
  [dataset & [column-name-seq]]
  (column-filter dataset #(not= 0 (ds-col/missing %))
                 column-name-seq))


(defn not
  [dataset & [column-name-seq]]
  (if-not (nil? column-name-seq)
    (->> (c-set/difference
          (set (ds/column-names dataset))
          (set (select-column-names dataset column-name-seq)))
         (ds/order-column-names dataset))
    (ds/column-names dataset)))


(defn and
  [dataset lhs-name-seq rhs-name-seq & more-column-filters]
  (->> (reduce (fn [all-column-names and-operator]
                 (c-set/intersection all-column-names
                                     (set (select-column-names
                                           (ds/select dataset
                                                      (c-set/intersection
                                                       all-column-names
                                                       (set (ds/column-names dataset)))
                                                      :all)
                                           and-operator))))
               (set (ds/column-names dataset))
               (concat [lhs-name-seq rhs-name-seq]
                       more-column-filters))
       (ds/order-column-names dataset)))


(defn or
  [dataset lhs-name-seq rhs-name-seq & more-column-filters]
  (->> (apply c-set/union
              (set (select-column-names dataset lhs-name-seq))
              (set (select-column-names dataset rhs-name-seq))
              (map (comp set select-column-names) more-column-filters))
       (ds/order-column-names dataset)))


(defn inference?
  "Is this column used as an inference target"
  [dataset & [column-name-seq]]
  (column-filter
   dataset
   (comp #(= % :inference) :column-type ds-col/metadata)
   column-name-seq))


(defn target?
  "Is this column used as an inference target"
  [dataset & [column-name-seq]]
  (inference? dataset column-name-seq))


(defn feature?
  "Is this column used as a feature column"
  [dataset & [column-name-seq]]
  (column-filter
   dataset
   (comp #(= % :feature) :column-type ds-col/metadata)
   column-name-seq))


(defn boolean-math-filter
  [dataset bool-fn lhs rhs args]
  (column-filter
   dataset
   #(apply bool-fn
           (->> (concat [lhs rhs]
                        args)
                (map (partial pipe-base/eval-math-fn
                              dataset
                              (ds-col/column-name %)))))))


(defn >
  [dataset lhs rhs & args]
  (boolean-math-filter dataset clojure.core/> lhs rhs args))


(defn >=
  [dataset lhs rhs & args]
  (boolean-math-filter dataset clojure.core/>= lhs rhs args))


(defn <
  [dataset lhs rhs & args]
  (boolean-math-filter dataset clojure.core/< lhs rhs args))


(defn <=
  [dataset lhs rhs & args]
  (boolean-math-filter dataset clojure.core/<= lhs rhs args))


(defn eq
  [dataset lhs rhs & args]
  (boolean-math-filter dataset clojure.core/= lhs rhs args))


(defn not-eq
  [dataset lhs rhs & args]
  (boolean-math-filter dataset clojure.core/not= lhs rhs args))
