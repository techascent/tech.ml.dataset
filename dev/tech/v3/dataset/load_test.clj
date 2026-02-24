(ns tech.v3.dataset.load-test)

(def old-load-lib @#'clojure.core/load-lib)

(def data (time (let [loading* (atom nil)
                      data* (atom [])]
                  (with-redefs [clojure.core/load-lib
                                (fn [prefix lib & options]
                                  (let [s (System/nanoTime)
                                        m {:parent @loading* :child lib}
                                        old-lib @loading*]
                                    (reset! loading* lib)
                                    ;; (println {:prefix prefix :lib lib :options options})
                                    (let [out (apply old-load-lib prefix lib options)]
                                      (swap! data* conj (assoc m :t (- (System/nanoTime) s)))
                                      (reset! loading* old-lib)
                                      out)))]
                    (require '[tech.v3.dataset :as ds]))
                  @data*)))

(comment
  (require '[tech.v3.dataset :as ds])
  (def data-ds (ds/->dataset data))

  (require '[ham-fisted.function :as hamf-fn])
  (defn process-data
    [data]
    (let [m (java.util.HashMap.)]
      (run! (fn [{:keys [parent child t]}]
              (.compute m child (hamf-fn/bi-function k v
                                  (+ (long t) (long (or v 0)))))
              (when parent 
                (.compute m parent (hamf-fn/bi-function k v
                                     (- (long (or v 0)) t)))))
            data)
      (->> (mapv (juxt key val) (.entrySet m))
           (sort-by second >))))
  
  (defn compute-loaded
    [data ns-name]
    (->> (get (group-by :child data)
              ns-name)
         (mapv :parent)))
  
  (compute-loaded data 'ham-fisted.protocols)
  (compute-loaded data 'tech.v3.dataset.io.nippy)

  (defn compute-loads
    [data ns-name]
    (->> (get (group-by :parent data)
              ns-name)
         (mapv :child)))

  (compute-loads data 'ham-fisted.lazy-noncaching)

  )
