(ns tech.ml.dataset.etl.column-filters
  "Column filters.  Public symbols exposed here should be symbols that can be used
  during column filtering."
  (:require [tech.ml.dataset :as ds]
            [tech.ml.dataset.column :as ds-col]
            [tech.ml.protocols.etl :as etl-proto]
            [tech.ml.utils :as utils]
            [tech.datatype.java-unsigned :as unsigned]
            [tech.ml.dataset.etl.impl.column-filters :as etl-base
             :refer [def-column-filter
                     basic-metadata-filter apply-numeric-column-filter
                     process-filter-args]
             :as filter-impl]
            [clojure.set :as c-set])
  (:refer-clojure :exclude [and or boolean? string? not
                            * > < >= <= ==]))


(defn execute-column-filter
  [dataset colfilter]
  (filter-impl/execute-column-filter dataset colfilter))


(defmacro ^:private register-all-dtype-filters
  []
  `(do
     ~@(for [dtype-kwd (concat [:boolean :string]
                             unsigned/datatypes)]
         `(filter-impl/def-exact-datatype-filter ~dtype-kwd))))


(register-all-dtype-filters)


(def-column-filter numeric?
  "Return the columns with numeric datatypes."
  (basic-metadata-filter
   #(utils/numeric-datatype? (:datatype %)) dataset args))


(def-column-filter not
  "(set/difference world arg-result)"
  (let [all-names (set (map ds-col/column-name (ds/columns dataset)))]
    (c-set/difference all-names
                      (-> (execute-column-filter
                           dataset
                           (first args))
                          set))))

(def-column-filter or
  "(apply set/union arg-results)"
  (apply c-set/union #{} (map (comp set (partial execute-column-filter dataset))
                              args)))


(def-column-filter and
  "(set/intersection arg-results)"
 (let [live-set (set (execute-column-filter dataset (first args)))]
   (->> (rest args)
        (reduce (fn [live-set arg]
                  (when-not (= 0 (count live-set))
                    (c-set/intersection live-set (set (execute-column-filter
                                                       (ds/select
                                                        dataset live-set :all)
                                                       arg)))))
                live-set))))


(def-column-filter categorical?
  "Return the set of columns that are categorical."
 (basic-metadata-filter :categorical? dataset args))


(def-column-filter target?
  "Set of columns that have the target attribute set."
 (basic-metadata-filter :target? dataset args))


(def-column-filter missing?
  "Return set of columns with any missing values."
 (->> (process-filter-args dataset args)
      (filter #(clojure.core/> (count (ds-col/missing %)) 0))
      (mapv ds-col/column-name)))


(def-column-filter *
  "Returns all columns in the dataset"
  (->> (process-filter-args dataset args)
       (map ds-col/column-name)))


(defmacro def-numeric-boolean-filter
  [filter-symbol]
  `(def-column-filter ~filter-symbol
     ~(str "Return the set of columns where "
           (name filter-symbol)
           " evaluates to true or 1.")
     (apply-numeric-column-filter ~(symbol "clojure.core" (name filter-symbol))
                                  ~'dataset ~'args)))


(def-numeric-boolean-filter >)
(def-numeric-boolean-filter <)
(def-numeric-boolean-filter >=)
(def-numeric-boolean-filter <=)

(def-column-filter ==
  "Return columns where the expression is equal."
  (apply-numeric-column-filter clojure.core/= dataset args))

(def-column-filter !=
  "Return columns where expression is no equal."
  (apply-numeric-column-filter not= dataset args))
