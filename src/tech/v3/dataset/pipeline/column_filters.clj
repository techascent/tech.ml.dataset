(ns ^:no-doc tech.ml.dataset.pipeline.column-filters
  (:require [tech.v2.datatype :as dtype]
            [tech.v2.datatype.casting :as casting]
            [tech.ml.dataset.base :as ds]
            [tech.ml.dataset.column :as ds-col]
            [tech.ml.dataset.pipeline.base :as pipe-base]
            [clojure.set :as c-set])
  (:refer-clojure :exclude [string? keyword? symbol? not and or boolean?
                            > >= < <=]))



(defn check-dataset
  [& [dataset]]
  (let [retval  (clojure.core/or dataset pipe-base/*pipeline-dataset*)]
    (when-not retval
      (throw (ex-info "Dataset is not bound; use with-ds" {})))
    retval))


(defn select-column-names
  [column-name-seq & [dataset]]
  (let [dataset (check-dataset dataset)]
    (pipe-base/with-ds dataset
      (cond
        (fn? column-name-seq)
        (column-name-seq)
        (clojure.core/or (= :all column-name-seq)
                         (sequential? column-name-seq)
                         (nil? column-name-seq))
        (->> (ds/select-columns dataset (clojure.core/or column-name-seq :all))
             ds/column-names)
        (clojure.core/or (clojure.core/string? column-name-seq)
                         (clojure.core/keyword? column-name-seq)
                         (clojure.core/symbol? column-name-seq))
        [column-name-seq]
        :else
        (throw (ex-info (format "Unrecognized argument to select columns: %s"
                                column-name-seq)
                        {}))))))


(defn select-columns
  [column-name-seq & [dataset]]
  (let [dataset (check-dataset dataset)]
    (->> (select-column-names column-name-seq dataset)
         (map (partial ds/column dataset)))))


(defn column-filter
  [col-filter-fn & [dataset]]
  (->> (select-columns nil dataset)
       (filter col-filter-fn)
       (map ds-col/column-name)))


(defn of-datatype?
  "Return column-names  of a given datatype."
  [dataset datatype]
  (column-filter #(= datatype (dtype/get-datatype %))
                 dataset))


(defn string?
  [& [dataset]]
  (of-datatype? dataset :string))


(defn keyword?
  [& [dataset]]
  (of-datatype? dataset :keyword))


(defn symbol?
  [& [dataset]]
  (of-datatype? dataset :keyword))


(defn string-or-keyword-or-symbol?
  [& [dataset]]
  (column-filter #(#{:string :keyword :symbol} (dtype/get-datatype %))
                 dataset))


(defn boolean?
  [& [dataset]]
  (of-datatype? dataset :boolean))


(defn numeric?
  [& [dataset]]
  (column-filter #(casting/numeric-type? (dtype/get-datatype %))
                 dataset))


(defn categorical?
  [& [dataset]]
  (column-filter (comp :categorical? ds-col/metadata) dataset))


(defn missing?
  [& [dataset]]
  (column-filter #(not= 0 (ds-col/missing %)) dataset))



(defn inference?
  "Is this column used as an inference target"
  [& [dataset]]
  (column-filter
   (comp #(= % :inference) :column-type ds-col/metadata)
   dataset))


(defn target?
  "Is this column used as an inference target"
  [& [dataset]]
  (inference? dataset))


(defn feature?
  "Is this column used as a feature column"
  [& [dataset]]
  (column-filter
   (comp #(= % :feature) :column-type ds-col/metadata)
   dataset))


(defn not
  [column-name-seq & [dataset]]
  (let [dataset (check-dataset dataset)]
    (pipe-base/with-pipeline-vars dataset nil nil nil
      (if-not (nil? column-name-seq)
        (->> (c-set/difference
              (set (ds/column-names dataset))
              (set (select-column-names column-name-seq)))
             (ds/order-column-names dataset))
        (ds/column-names dataset)))))


(defn and
  [lhs-name-seq rhs-name-seq & more-column-filters]
  (let [dataset (check-dataset)]
    (->> (reduce (fn [all-column-names and-operator]
                   (pipe-base/with-pipeline-vars
                     (ds/select dataset all-column-names :all) nil nil nil
                     (c-set/intersection all-column-names
                                         (set (select-column-names and-operator)))))
                 (set (ds/column-names dataset))
                 (concat [lhs-name-seq rhs-name-seq]
                         more-column-filters))
         (ds/order-column-names dataset))))


(defn or
  [lhs-name-seq rhs-name-seq & more-column-filters]
  (let [dataset (check-dataset)]
    (pipe-base/with-ds dataset
      (->> (apply c-set/union
                  (set (select-column-names lhs-name-seq))
                  (set (select-column-names rhs-name-seq))
                  (map (comp set select-column-names) more-column-filters))
           (ds/order-column-names dataset)))))


(defn boolean-math-filter
  [bool-fn lhs rhs args]
  (let [dataset (check-dataset)]
    (column-filter
     #(apply bool-fn
             (->> (concat [lhs rhs]
                          args)
                  (map (partial pipe-base/eval-math-fn
                                dataset
                                (ds-col/column-name %))))))))


(defn >
  [lhs rhs & args]
  (boolean-math-filter clojure.core/> lhs rhs args))


(defn >=
  [lhs rhs & args]
  (boolean-math-filter clojure.core/>= lhs rhs args))


(defn <
  [lhs rhs & args]
  (boolean-math-filter clojure.core/< lhs rhs args))


(defn <=
  [lhs rhs & args]
  (boolean-math-filter clojure.core/<= lhs rhs args))


(defn eq
  [lhs rhs & args]
  (boolean-math-filter clojure.core/= lhs rhs args))


(defn not-eq
  [lhs rhs & args]
  (boolean-math-filter clojure.core/not= lhs rhs args))


(defn numeric-and-non-categorical-and-not-target
  [& [dataset]]
  (let [dataset (check-dataset dataset)]
    (pipe-base/with-ds dataset
      (and #(not categorical?)
           #(not target?)
           numeric?))))
