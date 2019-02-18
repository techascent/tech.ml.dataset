(ns tech.ml.dataset.options
  "The etl pipeline and dataset operators are built to produce a metadata options map.
  Their API access to the options is centralized in this file."
  (:require [tech.ml.dataset.categorical :as categorical]))


(defn ->dataset-label-map
  [options]
  (:label-map options))


(defn ->column-label-map
  [options column-name]
  (if-let [retval (get-in options [:label-map column-name])]
    retval
    (throw (ex-info (format "Failed to find label map for column %s." column-name)
                    {:column-name column-name
                     :dataset-label-map (:label-map options)}))))


(defn has-column-label-map?
  [options column-name]
  (boolean (get-in options [:label-map column-name])))


(defn merge-label-maps
  [lhs-options rhs-options]
  (update lhs-options
          :label-map
          merge
          (:label-map rhs-options)))


(defn set-label-map
  [options lmap]
  (assoc options :label-map lmap))


(defn feature-column-names
  [options]
  (:feature-columns options))


(defn set-feature-column-names
  [options colname-seq]
  (assoc options :feature-columns colname-seq))


(defn label-column-names
  [options]
  (:label-columns options))


(defn set-label-column-names
  [options colname-seq]
  (assoc options :label-columns colname-seq))


(defn expand-column-names
  "In the case of one-hot encoding, the column names may be
  expanded and the original column removed."
  [options colname-seq]
  (->> colname-seq
       (mapcat (fn [colname]
                 (if (and (has-column-label-map? options colname)
                          (categorical/is-one-hot-label-map?
                           (->column-label-map options colname)))
                   (->> (->column-label-map options colname)
                        vals
                        (map first))
                   [colname])))))


(defn reduce-column-names
  "Inverse of expand column names.  Reverse map from the one-hot encoded columns
  to the original source column."
  [options colname-seq]
  (let [colname-set (set colname-seq)
        reverse-map (->> (->dataset-label-map options)
                         (mapcat (fn [[colname colmap]]
                                   ;;If this is one hot *and* every one hot is represented in the
                                   ;;column name sequence, then we can recover the original column.
                                   (when (and (categorical/is-one-hot-label-map? colmap)
                                              (every? colname-set (->> colmap
                                                                       vals
                                                                       (map first))))
                                     (->> (vals colmap)
                                          (map (fn [[derived-col col-idx]]
                                                 [derived-col colname]))))))
                         (into {}))]
    (->> colname-seq
         (map (fn [derived-name]
                (if-let [original-name (get reverse-map derived-name)]
                  original-name
                  derived-name)))
         distinct)))


(defn model-type-for-column
  [options column-name]
  (if (has-column-label-map? options column-name)
    :classification
    :regression))


(defn model-type-map
  "return a map of source label colname to model type."
  [options & [colname-seq]]
  (->> (or colname-seq (label-column-names options))
       (reduce-column-names options)
       (map (fn [colname]
              [colname (model-type-for-column options colname)]))
       (into {})))
