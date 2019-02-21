(ns tech.ml.dataset.etl.impl.math-ops
  (:require [tech.ml.dataset.etl.defaults :refer [etl-datatype]]
            [tech.ml.dataset.column :as ds-col]
            [tech.ml.dataset :as ds]
            [tech.ml.dataset.compute-math-context :as compute-math-context]
            [tech.compute.tensor :as ct]))


(defonce ^:dynamic *etl-math-ops* (atom {}))


(defonce ^:dynamic *col-math-context*
  (compute-math-context/->ComputeTensorMathContext))

(defn is-tensor? [op-arg]
  (ds-col/is-tensor? *col-math-context* op-arg))

(defn unary-op [op-arg op-kwd]
  (ds-col/unary-op *col-math-context* op-arg op-kwd))

(defn unary-reduction [op-arg op-kwd]
  (ds-col/unary-reduce *col-math-context* op-arg op-kwd))

(defn binary-op [op-args op-scalar-fn op-kwd]
  (ds-col/binary-op *col-math-context* op-args op-scalar-fn op-kwd))

(defn register-math-op!
  [op-kwd op-type op-fn]
  (swap! *etl-math-ops* assoc op-kwd {:type op-type
                                      :operand op-fn})
  (keys @*etl-math-ops*))


(defn get-operand
  "Return a map of (at least)
  {:type op-type
   :operand op-fn
  }"
  [op-kwd]
  (if-let [retval (get @*etl-math-ops* op-kwd)]
    retval
    (throw (ex-info (format "Failed to find math operand: %s" op-kwd)
                    {:operand op-kwd}))))


(defmacro def-math-op
  "General form of definition a math operation.  Creates a function of signature:
  (fn [{:keys [dataset column-name] :as op-env} & op-args] ...)"
  [op-sym optype docstring? & body]
  (let [body (if (string? docstring?)
               body
               (concat [docstring?] body))
        docstring? (if (string? docstring?)
                     docstring?
                     "")]
    `(do
       (defn ~op-sym
         ~docstring?
         [{:keys [~'dataset ~'column-name] :as ~'op-env} & ~'args]
         ~@body)
       (register-math-op! ~(keyword (name op-sym)) ~optype ~op-sym))))


(defmacro def-no-env-math-op
  "Specific form of defining a math operation that does not use the global environemnt."
  [op-sym optype docstring? & body]
  (let [body (if (string? docstring?)
               body
               (concat [docstring?] body))
        docstring? (if (string? docstring?)
                     docstring?
                     "")]
    `(do
       (defn ~op-sym
         ~docstring?
         [& ~'args]
         ~@body)
       (register-math-op! ~(keyword (name op-sym)) ~optype
                          (fn [_# & op-args#]
                            (apply ~op-sym op-args#))))))


(defn apply-unary-op
  [op-kwd scalar-fn op-args]
  (when-not (= 1 (count op-args))
    (throw (ex-info "Unary function applied to multiple or zero arguments"
                    {:op-args op-args})))
  (let [op-arg (first op-args)]
    (cond
      (ct/acceptable-tensor-buffer? op-arg)
      (ds-col/unary-op (ds-col/math-context op-arg) op-arg op-kwd)
      (number? op-arg)
      (scalar-fn (double op-arg))
      :else
      (throw (ex-info (format "Unrecognized unary argument type: %s" op-arg) {})))))


(defmacro def-unary
  [op-sym op-scalar-fn]
  `(def-no-env-math-op ~op-sym :unary
     ~(str "Perform unary function " (name op-sym))
     (apply-unary-op ~(keyword (name op-sym)) ~op-scalar-fn ~'args)))


(defn col-stat-unary
  [op-kwd args]
  (when-not (= 1 (count args))
    (throw (ex-info "Unary function applied to multiple or zero arguments"
                    {:op-args args})))
  (when-not (is-tensor? (first args))
    (throw (ex-info "Column stat function applied to something that is not a column."
                    {:argtype (type (first args))})))
  (-> (ds-col/stats (first args) [op-kwd])
      op-kwd))


(defmacro def-col-stat-unary
  [op-sym]
  `(def-no-env-math-op ~op-sym :unary
     ~(str "Apply column statistic function " (name op-sym))
     (col-stat-unary ~(keyword (name op-sym)) ~'args)))


(defn apply-binary-op
  [op-kwd scalar-fn op-args]
  (when-not (>= (count op-args) 1)
    (throw (ex-info "Binary operation called with fewer than 2 arguments."
                    {:operations op-kwd
                     :op-args op-args})))
  (if-let [any-tensors (->> op-args
                            (filter is-tensor?)
                            seq)]
    (ds-col/binary-op (ds-col/math-context (first any-tensors))
                      op-args scalar-fn op-kwd)
    (apply scalar-fn op-args)))


(defmacro def-binary
  [op-sym scalar-fn]
  `(def-no-env-math-op ~op-sym :binary
     ~(str "Apply binary function " (name op-sym))
     (apply-binary-op ~(keyword (name op-sym)) ~scalar-fn ~'args)))


(defn col-stat-unary-binary
  [op-kwd scalar-fn args]
  (if (and (= 1 (count args))
           (is-tensor? (first args)))
    (-> (ds-col/stats (first args) [op-kwd])
        op-kwd)
    (apply-binary-op op-kwd scalar-fn args)))


(defmacro def-col-stat-unary-binary
  [op-sym scalar-fn]
  `(def-no-env-math-op ~op-sym :binary
     ~(str (format "Either normal binary op %s or column stat fn %s"
                   (name op-sym) (name op-sym)))
     (col-stat-unary-binary ~(keyword (name op-sym))
                            ~scalar-fn ~'args)))


(defn eval-expr
  "Tiny simple interpreter."
  [{:keys [dataset column-name] :as env} math-expr]
  (cond
    (string? math-expr)
    math-expr
    (number? math-expr)
    math-expr
    (boolean? math-expr)
    math-expr
    (sequential? math-expr)
    (let [fn-name (first math-expr)
          ;;Force errors early
          expr-args (mapv (partial eval-expr env) (rest math-expr))
          {op-type :type
           operand :operand} (get-operand (keyword (name fn-name)))]
      (try
        (apply operand env expr-args)
        (catch Throwable e
          (throw (ex-info (format "Operator %s failed:\n%s" math-expr (.getMessage e))
                          {:math-expression math-expr
                           :error e})))))
    :else
    (throw (ex-info (format "Malformed expression %s" math-expr) {}))))
