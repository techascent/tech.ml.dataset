(ns tech.ml.dataset.bpd-test
  "The Boulder Police Department (BPD) releases the police call logs
  and they are a real mess.  This is makes an excellent test and demonstration
  of using the dataset library on a real world messy problem."
  (:require [tech.ml.dataset :as ds]
            [tech.ml.dataset.parse.datetime :as parse-dt]
            ;;Need appropriate missing value indicators
            [tech.ml.dataset.impl.column :as col-impl]
            [tech.ml.dataset.impl.dataset :as ds-impl]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.bitmap :as bitmap]
            [tech.v2.datatype.datetime :as dtype-dt]
            [tech.v2.datatype.typecast :as typecast]
            [tech.libs.nettoolkit :as nettoolkit]
            [clojure.tools.logging :as log]
            [clojure.test :refer [deftest is]])
  (:import [java.time LocalDateTime]
           [tech.v2.datatype LongReader ObjectReader]
           [java.util.concurrent ConcurrentHashMap]
           [java.util.function Function]
           [java.util Map List]
           [it.unimi.dsi.fastutil.doubles DoubleArrayList]))


(set! *warn-on-reflection* true)


(def test-file "test/data/BPD_Call_Log.csv")


(deftest bpd-date-format
  (is (instance? LocalDateTime (parse-dt/parse-local-date-time
                                "1/16/2018 8:34:03 AM"))))


(defn col->longs
  [col]
  (let [obj-rdr (typecast/datatype->reader :object col)]
    (reify LongReader
      (lsize [rdr] (.lsize obj-rdr))
      (read [rdr idx]
        (Long/parseLong (.read obj-rdr idx))))))


(defn col->packed-date-time
  [col]
  (let [obj-rdr (typecast/datatype->reader :object col)]
    (-> (reify ObjectReader
          (lsize [rdr] (.lsize obj-rdr))
          (read [rdr idx]
            (parse-dt/parse-local-date-time (.read obj-rdr idx))))
        (dtype-dt/pack))))


(defn initial->left-dataset
  [initial-ds]
  (let [correct-ds (ds/filter #(try
                                 (Long/parseLong (get % "Incident_ID"))
                                 (parse-dt/parse-local-date-time (get % "ResponseDate"))
                                 true
                                 (catch Throwable e false))
                              initial-ds
                              ["Incident_ID" "ResponseDate"])]
    (-> correct-ds
        (ds/update-column "ResponseDate" col->packed-date-time)
        (ds/update-column "Incident_ID" col->longs))))


(defn initial->right-dataset
  [initial-ds]
  (let [correct-ds (ds/filter #(try
                                 (Long/parseLong (get % "ResponseDate"))
                                 (parse-dt/parse-local-date-time (get % "Incident_ID"))
                                 true
                                 (catch Throwable e false))
                              initial-ds
                              ["Incident_ID" "ResponseDate"])
        rename-map {"CaseNumber" "Problem"
                    "HundredBlock" "CaseNumber"
                    "Problem" "HundredBlock"
                    "Incident_ID" "ResponseDate"
                    "ResponseDate" "Incident_ID"}]
    (-> correct-ds
        (ds/rename-columns rename-map)
        (ds/update-column "ResponseDate" col->packed-date-time)
        (ds/update-column "Incident_ID" col->longs))))


(defn preprocess-bpd-dataset
  ([fname]
   (let [initial-ds (ds/->dataset fname)
         final-ds (-> (ds/concat (initial->left-dataset initial-ds)
                                 (initial->right-dataset initial-ds))
                      (dtype/clone))]
     (ds/sort-by-column "ResponseDate" final-ds)))
  ([]
   (preprocess-bpd-dataset test-file)))


(defonce cached-geocode-results
  (ConcurrentHashMap.))


(defn geocode-bpd-dataset
  "Geocode every 100-block of addresses.
  Return new dataset of HundredBlock,Latitude,Longitude"
  [bpd-ds]
  (let [addresses (set (->> (bpd-ds "HundredBlock")
                            (filter #(not= "" %))))
        _ (log/infof "Geocoding %d addresses" (count addresses))
        compute-fn (reify Function
                     (apply [this addr]
                       (try
                         (let [{:keys [latitude longitude] :as result}
                               (-> (nettoolkit/geocode-address
                                    (str addr ",Boulder,CO"))
                                   (nettoolkit/geocode-address-result->lat-lng))]
                           (if (and latitude longitude)
                             result
                             (do
                               (log/warnf "Failure to unpack geocode result %s: %s"
                                          addr (with-out-str
                                                 (clojure.pprint/pprint result)))
                               nil)))
                         (catch Throwable e
                           (log/warnf "Failure to geocode address %s: %s"
                                      addr e)
                           nil))))
        double-missing-val (col-impl/datatype->missing-value :float64)
        lat-list (DoubleArrayList.)
        lng-list (DoubleArrayList.)
        ^List string-data (col-impl/make-container :string)
        missing (bitmap/->bitmap)]
    ;;Geocode all the things.  Result is our geocode map has the information.
    (->> (map-indexed vector (sort addresses))
         (pmap (fn [[idx addr]]
                 (when (= 0 (rem idx 100))
                   (log/infof "Geocoding address %d" idx))
                 [idx addr
                  (.computeIfAbsent ^Map cached-geocode-results addr compute-fn)]))
         (map (fn [[idx addr latlon]]
                (try
                  (.add string-data addr)
                  (if latlon
                    (do
                      (.add lat-list (double (:latitude latlon)))
                      (.add lng-list (double (:longitude latlon))))
                    (do
                      (.add missing (int idx))
                      (.add lat-list (double double-missing-val))
                      (.add lng-list (double double-missing-val))))
                  (catch Throwable e
                    (println "Error duing ds build step: " addr latlon)
                    (throw e)))))
         (dorun))
    ;;build small dataset out of just geocoded information.
    (ds-impl/new-dataset
     "Addresses" {}
     [(col-impl/new-column "HundredBlock" string-data
                           {})
      (col-impl/new-column "Latitude" lat-list {} missing)
      (col-impl/new-column "Longitude" lng-list {} missing)])))


(def geocoded-dataset (delay (ds/->dataset "geocoded-addresses.tsv.gz")))
