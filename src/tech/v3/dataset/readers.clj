(ns ^:no-doc tech.v3.dataset.readers
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.protocols.dataset :as ds-proto])
  (:import [tech.v3.datatype ObjectReader Buffer ListPersistentVector]
           [tech.v3.dataset FastStruct]
           [java.util List HashMap Collections ArrayList]))


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
  :copying? - Default to false - When true row values are copied on read."
  (^Buffer [dataset options]
   (let [readers (dataset->column-readers dataset)
         n-rows (long (second (dtype/shape dataset)))
         n-cols (long (first (dtype/shape dataset)))
         copying? (boolean (get options :copying?))]
     ;;we have a fast vector constructor in this case
     (if (and copying? (< n-cols 7))
       (case n-cols
         0 (reify ObjectReader
             (lsize [rdr] n-rows)
             (readObject [rdr row-idx] []))
         1 (let [c0 (.get readers 0)]
             (reify ObjectReader
               (lsize [rdr] n-rows)
               (readObject [rdr row-idx] [(c0 row-idx)])))
         2 (let [c0 (.get readers 0)
                 c1 (.get readers 1)]
             (reify ObjectReader
               (lsize [rdr] n-rows)
               (readObject [rdr row-idx] [(c0 row-idx) (c1 row-idx)])))
         3 (let [c0 (.get readers 0)
                 c1 (.get readers 1)
                 c2 (.get readers 2)]
             (reify ObjectReader
               (lsize [rdr] n-rows)
               (readObject [rdr row-idx] [(c0 row-idx) (c1 row-idx)
                                          (c2 row-idx)])))
         4 (let [c0 (.get readers 0)
                 c1 (.get readers 1)
                 c2 (.get readers 2)
                 c3 (.get readers 3)]
             (reify ObjectReader
               (lsize [rdr] n-rows)
               (readObject [rdr row-idx] [(c0 row-idx) (c1 row-idx)
                                          (c2 row-idx) (c3 row-idx)])))
         5 (let [c0 (.get readers 0)
                 c1 (.get readers 1)
                 c2 (.get readers 2)
                 c3 (.get readers 3)
                 c4 (.get readers 4)]
             (reify ObjectReader
               (lsize [rdr] n-rows)
               (readObject [rdr row-idx] [(c0 row-idx) (c1 row-idx)
                                          (c2 row-idx) (c3 row-idx)
                                          (c4 row-idx)])))
         6 (let [c0 (.get readers 0)
                 c1 (.get readers 1)
                 c2 (.get readers 2)
                 c3 (.get readers 3)
                 c4 (.get readers 4)
                 c5 (.get readers 5)]
             (reify ObjectReader
               (lsize [rdr] n-rows)
               (readObject [rdr row-idx] [(c0 row-idx) (c1 row-idx)
                                          (c2 row-idx) (c3 row-idx)
                                          (c4 row-idx) (c5 row-idx)]))))
       (if copying?
         (reify ObjectReader
           (lsize [rdr] n-rows)
           (readObject [rdr row-idx]
             (ListPersistentVector.
              (let [data (ArrayList. n-cols)]
                (dotimes [col-idx n-cols]
                  (.add data (.get ^List (.get readers col-idx) row-idx)))
                data))))
         (reify ObjectReader
           (lsize [rdr] n-rows)
           (readObject [rdr row-idx]
             ;;in-place reads
             (ListPersistentVector.
              (reify ObjectReader
                (lsize [this] n-cols)
                (readObject [this col-idx]
                  (.get ^List (.get readers col-idx) row-idx))))))))))
  (^Buffer [dataset]
   (value-reader dataset nil)))


(defn mapseq-reader
  "Return a reader that produces a map of column-name->column-value
  upon read."
  (^Buffer [dataset options]
   (let [colnamemap (HashMap.)
         _ (doseq [[c-name c-idx] (->> (ds-proto/columns dataset)
                                       (map (comp :name meta))
                                       (map-indexed #(vector %2 (int %1))))]
             (.put colnamemap c-name c-idx))
         colnamemap (Collections/unmodifiableMap colnamemap)
         copying? (get options :copying?)
         readers (value-reader dataset options)]

     (reify ObjectReader
       (lsize [rdr] (.lsize readers))
       (readObject [rdr idx]
         (let [data (readers idx)]
           (FastStruct. colnamemap (if (instance? ListPersistentVector data)
                                     (.data ^ListPersistentVector data)
                                     data)))))))
  (^Buffer [dataset]
   (mapseq-reader dataset nil)))
