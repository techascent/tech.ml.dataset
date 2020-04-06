(ns tech.ml.dataset.bpd-test
  "The Boulder Police Department (BPD) releases the police call logs
  and they are a real mess.  This is makes an excellent test and demonstration
  of using the dataset library on a real world messy problem."
  (:require [tech.ml.dataset :as ds]
            [tech.ml.dataset.parse.datetime :as parse-dt]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.datetime :as dtype-dt]
            [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.functional :as dfn]
            [clojure.test :refer [deftest is]])
  (:import [java.time LocalDateTime]
           [tech.v2.datatype LongReader ObjectReader]))


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
