(ns tech.v3.dataset.impl.column-data-process
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.bitmap :as bitmap]
            [tech.v3.parallel.for :as parallel-for]
            [tech.v3.protocols.column :as col-proto]
            [tech.v3.dataset.io.column-parsers :as column-parsers]
            [tech.v3.dataset.impl.column-base :as column-base])
  (:import [org.roaringbitmap RoaringBitmap]
           [java.util Map Objects]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defmacro missing-value?
  "Is this a missing value given the datatype-specific column missing value"
  [missing-value obj]
  `(and (not (instance? Boolean ~obj))
        (or (nil? ~obj)
            (Objects/equals ~missing-value ~obj)
            (identical? :tech.v3.dataset/missing ~obj)
            (identical? :tech.v3.dataset/parse-failure ~obj))))


(defn scan-missing
  "Scan a (potentially primitive) reader for missing values.  This simply scans every
  value in the reader."
  ^RoaringBitmap [rdr]
  (let [rdr (dtype/->reader rdr)
        missing (column-base/datatype->missing-value (dtype/elemwise-datatype rdr))]
    (parallel-for/indexed-map-reduce
     (.lsize rdr)
     (fn [^long start-idx ^long group-len]
       (let [bmp (bitmap/->bitmap)]
         (dotimes [group-idx group-len]
           (let [idx (+ group-idx start-idx)
                 obj (.readObject rdr idx)]
             (when (missing-value? missing obj)
               (.add bmp idx))))
         bmp))
     (fn [bmps]
       (let [^RoaringBitmap bmp (first bmps)]
         (doseq [^RoaringBitmap other-bmp (rest bmps)]
           (.or bmp other-bmp))
         bmp)))))


(defn scan-data
  [obj-data missing]
  (let [obj-data-datatype (dtype/elemwise-datatype obj-data)]
    (if (and (dtype/reader? obj-data)
             (not= :object obj-data-datatype))
      ;;If the user knows the datatype they want, then we just scan for missing if it
      ;;wasn't provided.
      #:tech.v3.dataset{:data obj-data
                        :force-datatype? true
                        :missing (or missing
                                     ;;integer types really have no meaningful missing value
                                     ;;indicator so we don't scan for that.
                                     (if-not (or
                                              (casting/unsigned-integer-type?
                                               obj-data-datatype)
                                              (casting/integer-type? obj-data-datatype))
                                       (scan-missing obj-data)
                                       (bitmap/->bitmap)))}
      (let [obj-meta (meta obj-data)
            parser (column-parsers/promotional-object-parser
                    (:name obj-meta) obj-meta)
            ;;At this point either you are convertible to a reader or you are
            ;;iterable.
            obj-data (or (dtype/as-reader obj-data) obj-data)
            missing (bitmap/->bitmap missing)]
        (if-let [rdr (dtype/as-reader obj-data)]
          (dotimes [idx (.lsize rdr)]
            (column-parsers/add-value! parser idx
                                       (when-not (.contains missing idx)
                                         (.readObject rdr idx))))
          ;;serially consume the data promoting the container when necessary.
          (parallel-for/indexed-consume!
           ;;Do not read from a missing entry if missing is provided
           #(column-parsers/add-value! parser %1
                                       (when-not (.contains missing (unchecked-int %1))
                                         %2))
           obj-data))
        (column-parsers/finalize! parser (dtype/ecount parser))))))


(defn prepare-column-data
  "Scan data for missing values and to infer storage datatype.  Returns
   a map of least #tech.v3.dataset{:data :missing :force-datatype?}."
  [obj-data]
  (cond
    (map? obj-data)
    (do
      (errors/when-not-errorf
       (contains? ^Map obj-data :tech.v3.dataset/data)
       "Map constructors must contain at least :tech.v3.dataset/data")
      ;;ensure we do not re-scan this object
      (let [{:keys [tech.v3.dataset/data
                    tech.v3.dataset/missing
                    tech.v3.dataset/force-datatype?]} obj-data
            obj-data (if (nil? missing)
                       (dissoc obj-data :tech.v3.dataset/missing)
                       obj-data)]
        (merge
         (if (and (dtype/reader? data) force-datatype?)
           ;;skip scan of the data, but potentially still scan for missing
           {:tech.v3.dataset/data data
            :tech.v3.dataset/missing (if missing missing (scan-missing data))}
           (scan-data data missing))
         ;;allow missing/metadata to make it through so user can override
         (dissoc obj-data :tech.v3.dataset/data)
         #:tech.v3.dataset{:force-datatype? true})))
    (col-proto/is-column? obj-data)
    (col-proto/as-map obj-data)
    :else
    (scan-data obj-data nil)))


(defn prepare-column-data-seq
  [column-seq]
  (pmap (fn [idx item]
          (-> (if (map? item)
                (update item :tech.v3.dataset/name
                        #(if (nil? %)
                           idx %))
                item)
              (prepare-column-data)))
        (range) column-seq))
