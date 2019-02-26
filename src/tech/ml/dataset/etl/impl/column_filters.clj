(ns tech.ml.dataset.etl.impl.column-filters
  "Base namespace that defines the mechanisms behind the column filtering system."
    (:require [tech.ml.dataset :as ds]
              [tech.ml.dataset.column :as ds-col]
              [tech.ml.dataset.etl.impl.math-ops :as math-ops]
              [tech.datatype.java-unsigned :as unsigned]))


(defonce ^:dynamic *column-filter-fns* (atom {}))


(defn register-column-filter!
  "The column filters namepsace contains an API function, `registing-column-filter!`.
  The symbol column filter name is converted to a keyword and looked up in a global map.

The filter-function has an api of `(dataset & arguments)` where the arguments are passed
  after evaluation to the filter function.  The return value is expected to be either a
  sequence of column names or a set of column names."
  [name-kwd filter-fn]
  (-> (swap! *column-filter-fns* assoc name-kwd filter-fn)
      keys))


(defmacro def-column-filter
  [filter-symbol docstring? & body]
  (let [body (if (string? docstring?)
               body
               (concat [docstring?] body))
        docstring? (if (string? docstring?)
                     docstring?
                     "")]
    `(do
       (defn ~filter-symbol
         ~docstring?
         [~'dataset & ~'args]
         ~@body)
       (register-column-filter!
          ~(keyword (name filter-symbol))
          ~filter-symbol))))


(defn- execute-col-filter-fn
  [dataset filter-fn args]
  (if-let [filter-op (get @*column-filter-fns* filter-fn)]
    (apply filter-op dataset args)
    (throw (ex-info (format "Failed to find column filter fn: %s"
                            filter-fn)
                    {:filter-fn filter-fn}))))


(defn- execute-column-filter
  [dataset col-filter]
  (cond
    (sequential? col-filter)
    (if (seq col-filter)
      (let [filter-fn (-> (first col-filter)
                          name
                          keyword)]
        (execute-col-filter-fn dataset filter-fn (rest col-filter)))
      [])
    (set? col-filter)
    col-filter
    (or (symbol? col-filter) (keyword? col-filter))
    (execute-col-filter-fn dataset (keyword (name col-filter)) [])
    :else
    (throw (ex-info "Unrecognized column filter symbol" {:filter col-filter}))))


(defn select-columns
  [dataset col-selector]
  (cond
    (string? col-selector)
    [col-selector]
    (keyword? col-selector)
    [col-selector]
    (sequential? col-selector)
    (if (or (string? (first col-selector))
            (keyword? (first col-selector)))
      col-selector
      (execute-column-filter dataset col-selector))
    (set? col-selector)
    col-selector
    (symbol? col-selector)
    (execute-column-filter dataset col-selector)
    :else
    (throw (ex-info (format "Unrecognized column selector %s" col-selector) {}))))


(defn process-filter-args
  [dataset args]
  (if (seq args)
    (map (partial ds/column dataset) args)
    (ds/columns dataset)))


(defn basic-metadata-filter
  [filter-fn dataset args]
  (->> (process-filter-args dataset args)
       (map (comp ds-col/metadata))
       (filter filter-fn)
       (map :name)))


(defmacro def-exact-datatype-filter
  [dtype-kwd]
  (let [filter-sym (symbol (str (name dtype-kwd) "?"))]
    `(def-column-filter ~filter-sym
       ~(format "Filter columns of datatype %s" (name dtype-kwd))
       (basic-metadata-filter #(= ~dtype-kwd (:datatype %))
                              ~'dataset ~'args))))


(defn apply-numeric-column-filter
  [op-fn dataset args]
  (when-not (= (count args) 2)
    (throw (ex-info "Boolean numeric filters take 2 arguments."
                    {:boolean-args args})))
   (->> (ds/columns dataset)
        (filter (fn [col]
                  (let [math-env {:dataset dataset
                                  :column-name (ds-col/column-name col)}]
                    (apply op-fn (->> args
                                      (map #(math-ops/eval-expr math-env %)))))))
        (mapv ds-col/column-name)))
