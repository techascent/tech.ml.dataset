(ns tech.v3.libs.guava.cache
  "Use a google guava cache to memoize function results.  Function must not return
  nil values.  Exceptions propagate to caller."
  (:refer-clojure :exclude [memoize])
  (:import [com.google.common.cache
            Cache CacheBuilder CacheLoader
            LoadingCache CacheStats]
           [java.time Duration]))

(set! *warn-on-reflection* true)


(defn memoize
  "Create a threadsafe, efficient memoized function using a guavacache backing store."
  [f & {:keys [write-ttl-ms
               access-ttl-ms
               soft-values?
               weak-values?
               max-size
               record-stats?]}]
  (let [^CacheBuilder new-builder
        (cond-> (CacheBuilder/newBuilder)
          access-ttl-ms
          (.expireAfterAccess (Duration/ofMillis access-ttl-ms))
          write-ttl-ms
          (.expireAfterWrite (Duration/ofMillis write-ttl-ms))
          soft-values?
          (.softValues)
          weak-values?
          (.weakValues)
          max-size
          (.maximumSize (long max-size))
          record-stats?
          (.recordStats))
        ^LoadingCache cache
        (.build new-builder
                (proxy [CacheLoader] []
                  (load [args]
                    (if-let [retval (apply f args)]
                      retval
                      (throw (ex-info
                              (format "Nil values not allowed in cache: %s" args)
                              {}))))))]
    (-> (fn [& args]
          (.get cache args))
        (with-meta {:cache cache}))))


(defn memo-stats
  "Get cache stats from a memoized object."
  [filecache]
  (let [^Cache memo-cache (-> (meta filecache)
                              :cache)
        ^CacheStats cache-map (.stats memo-cache)]
    {:hit-count (.hitCount cache-map)
     :hit-rate (.hitRate cache-map)
     :miss-count (.missCount cache-map)
     :miss-rate (.missRate cache-map)
     :load-success-count (.loadSuccessCount cache-map)
     :load-exception-count (.loadExceptionCount cache-map)
     :average-load-penalty-nanos (.averageLoadPenalty cache-map)
     :total-load-time-nanos (.totalLoadTime cache-map)
     :eviction-count (.evictionCount cache-map)}))
