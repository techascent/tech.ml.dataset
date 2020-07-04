(ns tech.ml.dataset.text.bag-of-words
  (:require [tech.ml.dataset.parse :as ds-parse]
            [tech.ml.dataset.base :as ds-base])
  (:import [smile.nlp.tokenizer SimpleTokenizer]
           [java.util HashMap Map]
           [java.util.function BiFunction]))


(set! *warn-on-reflection* true)


(defn sha256 ^String [^String string]
  (let [digest (.digest (java.security.MessageDigest/getInstance "SHA-256")
                        (.getBytes string "UTF-8"))]
    (apply str (map (partial format "%02x") digest))))


(defn simple-tokenizer-fn
  []
  (let [tkzr (SimpleTokenizer.)]
    (fn [^String str]
      (.split tkzr str))))


(def sum-bifun
  (reify BiFunction
    (apply [this a b]
      (+ (long a) (long b)))))


(defn parse-token-column
  [^Map token-table tokenizer col-data]
  (if (or (nil? col-data)
          (= "" col-data))
    :tech.ml.dataset.parse/missing
    (let [retval (sha256 col-data)
          tokens (tokenizer col-data)]
      (doseq [token tokens]
        (.merge token-table token 1 ^BiFunction sum-bifun))
      retval)))


(defn path->dataset-master-token-table
  "Parse a file returning a map of {:dataset :token-table} where token-table
  is a map of tokens to counts.  Dataset has a sha-256-hash where the original
  text once was."
  ([path bag-of-words-colname {:keys [tokenizer]
                               :or {tokenizer (simple-tokenizer-fn)}}]
   (let [token-table (HashMap.)
         retval-ds (ds-base/->dataset
                    path
                    {:parser-fn {bag-of-words-colname
                                 [:string (partial parse-token-column
                                                   token-table
                                                   tokenizer)]}})]
     {:dataset retval-ds
      ;;Make a normal persistent hashmap out of the hashmap.  This works better with
      ;;lots of clojure paradigms
      :token-table (into {} token-table)}))
  ([path bag-of-words-colname]
   (path->dataset-master-token-table path bag-of-words-colname {})))


(defn path-token-map->bag-of-words
  ([path bag-of-words-colname token->idx-map
     {:keys [tokenizer]
      :or {tokenizer (simple-tokenizer-fn)}}]
   (->> (ds-parse/csv->rows path {:column-whitelist ["abstract"]})
        ;;Drop column name.
        (drop 1)
        ;;Now we have a sequence of string arrays which with 1 entry
        (pmap (fn [^"[Ljava.lang.String;" str-data]
                (let [coldata (aget str-data 0)]
                  (when-not (or (nil? coldata)
                                (= "" coldata))
                    (let [tokens (tokenizer coldata)
                          doc-id (sha256 coldata)]
                      (->> tokens
                           (map #(get token->idx-map %))
                           (remove nil?)
                           (map #(hash-map :document-id doc-id
                                           :token-idx %))))))))
        (apply concat)
        (ds-base/->>dataset)))
  ([path bag-of-words-colname token->idx-map]
   (path-token-map->bag-of-words path bag-of-words-colname token->idx-map {})))
