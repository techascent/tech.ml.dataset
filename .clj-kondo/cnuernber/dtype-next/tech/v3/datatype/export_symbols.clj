(ns tech.v3.datatype.export-symbols
  (:require [clj-kondo.hooks-api :as hooks-api]))

(defmacro export-symbols
  [src-ns & symbol-list]
  (let [analysis (:clj (hooks-api/ns-analysis src-ns))]
    `(do
       ~@(->> symbol-list
              (mapv
               (fn [sym-name]
                 (when-let [fn-data (get analysis sym-name)]
                   (if-let [arities (get fn-data :fixed-arities)]
                     `(defn ~sym-name
                        ~@(->> arities
                               (map (fn [arity]
                                      (let [argvec (mapv
                                                    #(symbol (str "arg-" %))
                                                    (range arity))]
                                        `(~argvec (apply + ~argvec)
                                          ;;This line is to disable the type detection of clj-kondo
                                                  ~(if-not (= 0 arity)
                                                     (first argvec)
                                                     :ok)))))))
                     `(def ~sym-name :defined)))))))))
