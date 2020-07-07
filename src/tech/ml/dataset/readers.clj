(ns ^:no-doc tech.ml.dataset.readers
  (:require [tech.v2.datatype :as dtype]
            [tech.v2.datatype.datetime :as dtype-dt]
            [tech.v2.datatype.typecast :as typecast]
            [tech.ml.protocols.dataset :as ds-proto]
            [tech.ml.dataset.column :as ds-col])
  (:import [tech.v2.datatype ObjectReader]
           [tech.ml.dataset FastStruct]
           [org.roaringbitmap RoaringBitmap]
           [java.util List HashMap Collections ArrayList]))


(defn dataset->column-readers
  "Create a list of object column readers.  Packed datatypes will be unpacked.

  options -
  :missing-nil? - Default to true - Substitute nil in for missing values to make
    missing value detection downstream to be column datatype independent."
  (^List [dataset {:keys [missing-nil?]
                   :or {missing-nil? true}
                   :as _options}]
   (->> (ds-proto/columns dataset)
        (mapv (fn [coldata]
                (let [col-reader (dtype/->reader coldata)
                      ^RoaringBitmap missing (dtype/as-roaring-bitmap
                                              (ds-col/missing coldata))
                      col-rdr (typecast/datatype->reader
                               :object
                               (if (dtype-dt/packed-datatype?
                                    (dtype/get-datatype col-reader))
                                 (dtype-dt/unpack col-reader)
                                 col-reader))]
                  (if missing-nil?
                    (dtype/object-reader
                     (.size col-rdr)
                     (fn [^long idx]
                       (when-not (.contains missing idx)
                         (.read col-rdr idx))))
                    col-rdr))))))
  (^List [dataset]
   (dataset->column-readers dataset {})))


(defn value-reader
  "Return a reader that produces a reader of column values per index.
  Options:
  :missing-nil? - Default to true - Substitute nil in for missing values to make
    missing value detection downstream to be column datatype independent."
  (^ObjectReader [dataset options]
   (let [readers (dataset->column-readers dataset options)
         n-rows (long (second (dtype/shape dataset)))
         n-cols (long (first (dtype/shape dataset)))]
     (dtype/object-reader
      n-rows
      (fn [^long idx]
        (dtype/object-reader
         n-cols
         (fn [^long inner-idx]
           (.get ^List (.get readers inner-idx) idx)))))))
  (^ObjectReader [dataset]
   (value-reader dataset {})))


(defn mapseq-reader
  "Return a reader that produces a map of column-name->column-value

  Options:
  :missing-nil? - Default to true - Substitute nil in for missing values to make
    missing value detection downstream to be column datatype independent."
  (^ObjectReader [dataset options]
   (let [colnamemap (HashMap.)
         _ (doseq [[c-name c-idx] (->> (ds-proto/column-names dataset)
                                      (map-indexed #(vector %2 (int %1))))]
             (.put colnamemap c-name c-idx))
         colnamemap (Collections/unmodifiableMap colnamemap)
         readers (value-reader dataset options)]
     (reify ObjectReader
       (lsize [rdr] (.lsize readers))
       (read [rdr idx]
         (FastStruct. colnamemap (.read readers idx))))))
  (^ObjectReader [dataset]
   (mapseq-reader dataset {})))
