(ns ^:no-doc tech.ml.dataset.parse.name-values-seq
  (:require [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.casting :as casting]
            [tech.ml.dataset.parse.mapseq :as parse-mapseq]
            [tech.ml.dataset.impl.dataset :as ds-impl]
            [tech.ml.dataset.impl.column :as ds-col-impl]
            [tech.ml.protocols.dataset :as ds-proto]))


(defn parse-nvs
  [name-values-seq options]
  (let [name-order (map first name-values-seq)
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
                                   (ds-col-impl/ensure-column values-seq)
                                   (ds-col-impl/new-column colname values-seq))))
                          (ds-impl/new-dataset options))
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
                      (parse-mapseq/mapseq->dataset values options))]
    (-> (ds-impl/new-dataset options
                             (concat (ds-proto/columns half-dataset)
                                     (when leftover-ds
                                       (ds-proto/columns leftover-ds))))
        (ds-proto/select name-order :all))))


(defn name-values-seq->dataset
  "Given a sequence of [name data-seq], produce a columns.  If data-seq is
  of unknown (:object) datatype, the first item is checked. If it is a number,
  then doubles are used.  If it is a string, then strings are used for the
  column datatype.
  All sequences must be the same length.
  Returns a new dataset"
  [name-values-seq & {:as options}]
  (parse-nvs name-values-seq options))
