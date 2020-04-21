(ns tech.ml.dataset.parse.name-values-seq
  (:require [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.casting :as casting]
            [tech.ml.dataset.parse.mapseq :as parse-mapseq]
            [tech.ml.dataset.impl.dataset :as ds-impl]
            [tech.ml.protocols.dataset :as ds-proto]
            [tech.ml.dataset.column :as ds-col]))


(defn name-values-seq->dataset
  "Given a sequence of [name data-seq], produce a columns.  If data-seq is
  of unknown (:object) datatype, the first item is checked. If it is a number,
  then doubles are used.  If it is a string, then strings are used for the
  column datatype.
  All sequences must be the same length.
  Returns a new dataset"
  [name-values-seq & {:keys [dataset-name]
                      :or {dataset-name "_unnamed"}}]
  (let [sizes (->> (map (comp dtype/ecount second) name-values-seq)
                   distinct)
        _ (when-not (= 1 (count sizes))
            (throw (ex-info (format "Different sized columns detected: %s" sizes) {})))
        name-order (map first name-values-seq)
        ;;Allow explicit missing/etc to be passed in.
        map-data (filter (comp map? second) name-values-seq)
        ;;fastpaths for primitive arrays - no need to scan the data.
        known-container-data (filter (comp casting/numeric-type?
                                           dtype/get-datatype
                                           second)
                                     name-values-seq)
        half-dataset (->> (concat map-data known-container-data)
                          (map (fn [[colname values-seq]]
                                 (if (map? values-seq)
                                   (ds-col/ensure-column values-seq)
                                   (ds-col/new-column colname values-seq))))
                          (ds-impl/new-dataset dataset-name))
        colname-set (set (ds-proto/column-names half-dataset))
        leftover (remove (comp colname-set first) name-values-seq)
        n-data (count leftover)
        colnames (map first leftover)
        ;;Object columns mean we have to scan everything manually.
        values (->> (map second leftover)
                    (apply interleave)
                    (partition n-data)
                    (map #(zipmap colnames %)))
        leftover-ds (when (seq colnames)
                      (parse-mapseq/mapseq->dataset values))]
    (-> (ds-impl/new-dataset dataset-name
                             {}
                             (concat (ds-proto/columns half-dataset)
                                     (when leftover-ds
                                       (ds-proto/columns leftover-ds))))
        (ds-proto/select name-order :all))))
