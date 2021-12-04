(ns ^:no-doc tech.v3.dataset.readers
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.protocols.dataset :as ds-proto])
  (:import [tech.v3.datatype ObjectReader Buffer]
           [tech.v3.dataset FastStruct]
           [java.util List HashMap Collections]))


(defn dataset->column-readers
  "Create a list of object column readers.  Packed datatypes will be unpacked.

  options -
  :missing-nil? - Default to true - Substitute nil in for missing values to make
    missing value detection downstream to be column datatype independent."
  (^List [dataset]
   (->> (ds-proto/columns dataset)
        (mapv dtype/->reader))))


(defn value-reader
  "Return a reader that produces a reader of column values per index.
  Options:
  :missing-nil? - Default to true - Substitute nil in for missing values to make
    missing value detection downstream to be column datatype independent."
  (^Buffer [dataset]
   (let [readers (dataset->column-readers dataset)
         n-rows (long (second (dtype/shape dataset)))
         n-cols (long (first (dtype/shape dataset)))]
     (reify ObjectReader
       (lsize [rdr] n-rows)
       (readObject [rdr idx]
         (reify ObjectReader
           (lsize [rdr] n-cols)
           (readObject [rdr inner-idx]
             (.get ^List (.get readers inner-idx) idx))))))))


(defn mapseq-reader
  "Return a reader that produces a map of column-name->column-value

  Options:
  :missing-nil? - Default to true - Substitute nil in for missing values to make
    missing value detection downstream to be column datatype independent."
  (^Buffer [dataset]
   (let [colnamemap (HashMap.)
         _ (doseq [[c-name c-idx] (->> (ds-proto/columns dataset)
                                       (map (comp :name meta))
                                       (map-indexed #(vector %2 (int %1))))]
             (.put colnamemap c-name c-idx))
         colnamemap (Collections/unmodifiableMap colnamemap)
         readers (value-reader dataset)]
     (reify ObjectReader
       (lsize [rdr] (.lsize readers))
       (readObject [rdr idx]
         (FastStruct. colnamemap (readers idx)))))))
