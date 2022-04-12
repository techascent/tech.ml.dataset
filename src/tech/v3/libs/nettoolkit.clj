(ns ^:no-doc tech.v3.libs.nettoolkit
  (:require [org.httpkit.client :as http]
            [charred.api :as json]
            [clojure.pprint :as pp])
  (:import [java.nio.file Paths]
           [java.net URLEncoder]))


(defn combine-paths
  ^String [& args]
  (.toString ^Object (Paths/get (first args)
                                (into-array String (rest args)))))

(def nettoolkit-default-token
  (delay (slurp (combine-paths (System/getProperty "user.home")
                               ".nettoolkit-token"))))


(defn http-get
  ([url {:keys [nettoolkit-token]
         :as options}]
   (let [token (or nettoolkit-token
                   @nettoolkit-default-token)]
     (http/get url (merge {:headers {"X-NTK-KEY" token}}
                          (dissoc options :nettoolkit-token)))))
  ([url]
   (http-get url {})))


(defn- json-body
  [result]
  (if (= (:status result) 200)
    (json/read-json (:body result) :key-fn keyword)
    (throw (Exception. (format "Request failed\n%s"
                               (with-out-str
                                 (pp/pprint result)))))))


(defn geocode-address
  [addr]
  (-> @(http-get (str "https://api.nettoolkit.com/v1/geo/geocodes?address="
                      (URLEncoder/encode addr)))
      (json-body)))


(defn geocode-address-result->lat-lng
  [result]
  (-> (:results result)
      (first)
      (select-keys [:latitude :longitude])))
