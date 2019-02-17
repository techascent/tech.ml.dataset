(ns tech.ml.dataset
  "Column major dataset abstraction for efficiently manipulating
  in memory datasets."
  (:require [tech.datatype :as dtype]
            [tech.ml.dataset.column :as ds-col]
            [tech.ml.protocols.dataset :as ds-proto]
            [tech.parallel :as parallel]
            [clojure.core.matrix :as m]
            [clojure.core.matrix.macros :refer [c-for]]
            [clojure.set :as c-set])
  (:import [smile.clustering KMeans GMeans XMeans PartitionClustering]))


(set! *warn-on-reflection* true)


(defn dataset-name
  [dataset]
  (ds-proto/dataset-name dataset))

(defn maybe-column
  "Return either column if exists or nil."
  [dataset column-name]
  (ds-proto/maybe-column dataset column-name))


(defn column
  "Return the column or throw if it doesn't exist."
  [dataset column-name]
  (if-let [retval (maybe-column dataset column-name)]
    retval
    (throw (ex-info (format "Failed to find column: %s" column-name)
                    {:column-name column-name}))))

(defn columns
  "Return sequence of all columns in dataset."
  [dataset]
  (ds-proto/columns dataset))


(defn columns-with-missing-seq
  "Return a sequence of:
  {:column-name column-name
   :missing-count missing-count
  }
  or nil of no columns are missing data."
  [dataset]
  (->> (columns dataset)
       (map (fn [col]
              (let [missing-count (count (ds-col/missing col))]
                (when-not (= 0 missing-count)
                  {:column-name (ds-col/column-name col)
                   :missing-count missing-count}))))
       (remove nil?)
       seq))


(defn add-column
  "Add a new column. Error if name collision"
  [dataset column]
  (ds-proto/add-column dataset column))


(defn remove-column
  "Fails quietly"
  [dataset col-name]
  (ds-proto/remove-column dataset col-name))


(defn update-column
  "Update a column returning a new dataset.  update-fn is a column->column
  transformation.  Error if column does not exist."
  [dataset col-name update-fn]
  (ds-proto/update-column dataset col-name update-fn))


(defn add-or-update-column
  "If column exists, replace.  Else append new column."
  [dataset column]
  (ds-proto/add-or-update-column dataset column))


(defn select
  "Reorder/trim dataset according to this sequence of indexes.  Returns a new dataset.
colname-seq - either keyword :all or list of column names with no duplicates.
index-seq - either keyword :all or list of indexes.  May contain duplicates."
  [dataset colname-seq index-seq]
  (ds-proto/select dataset colname-seq index-seq))


(defn select-columns
  [dataset col-name-seq]
  (select dataset col-name-seq :all))


(defn index-value-seq
  "Get a sequence of tuples:
  [idx col-value-vec]

Values are in order of column-name-seq.  Duplicate names are allowed and result in
duplicate values."
  [dataset]
  (ds-proto/index-value-seq dataset))


(defn supported-column-stats
  "Return the set of natively supported stats for the dataset.  This must be at least
#{:mean :variance :median :skew}."
  [dataset]
  (ds-proto/supported-column-stats dataset))


(defn from-prototype
  "Create a new dataset that is the same type as this one but with a potentially
different table name and column sequence.  Take care that the columns are all of
the correct type."
  [dataset table-name column-seq]
  (ds-proto/from-prototype dataset table-name column-seq))


(defn ds-filter
  [dataset predicate & [column-name-seq]]
  ;;interleave, partition count would also work.
  (->> (index-value-seq (select dataset (or column-name-seq :all) :all))
       (filter (fn [[idx col-values]]
                 (apply predicate col-values)))
       (map first)
       (select dataset :all)))


(defn ds-group-by
  [dataset key-fn & [column-name-seq]]
  (->> (index-value-seq (select dataset (or column-name-seq :all) :all))
       (group-by (fn [[idx col-values]]
                   (apply key-fn col-values)))
       (map first)
       (select dataset :all)))


(defn ds-concat
  [dataset & other-datasets]
  (let [column-list
        (->> (concat [dataset] (remove nil? other-datasets))
             (mapcat (fn [dataset]
                       (->> (columns dataset)
                            (mapv (fn [col]
                                    (assoc (ds-col/metadata col)
                                           :column
                                           col
                                           :table-name (dataset-name dataset)))))))
                         (group-by :name))]
    (when-not (= 1 (count (->> (vals column-list)
                               (map count)
                               distinct)))
      (throw (ex-info "Dataset is missing a column" {})))
    (->> column-list
         (mapv (fn [[colname columns]]
                 (let [columns (map :column columns)
                       newcol-ecount (apply + 0 (map m/ecount columns))
                       first-col (first columns)
                       new-col (ds-col/new-column first-col
                                                     (dtype/get-datatype first-col)
                                                     newcol-ecount
                                                     (ds-col/metadata first-col))]
                   (dtype/copy-raw->item! (map ds-col/column-values columns)
                                          new-col 0
                                          {:unchecked? true})
                   new-col)))
         (ds-proto/from-prototype dataset (dataset-name dataset)))))


(defn ds-map
  "Note this returns a sequence, not a dataset."
  [dataset map-fn & [column-name-seq]]
  (->> (index-value-seq (select dataset (or column-name-seq :all) :all))
       (map (fn [[idx col-values]]
              (apply map-fn col-values)))))


(defn correlation-table
  "Return a map of colname->list of sorted tuple of [colname, coefficient].
  Sort is:
  (sort-by (comp #(Math/abs (double %)) second) >)

  Thus the first entry is:
  [colname, 1.0]

  There are three possible correlation types:
  :pearson
  :spearman
  :kendall

  :pearson is the default."
  [dataset & [correlation-type]]
  (let [colseq (columns dataset)
        correlation-type (or :pearson correlation-type)]
    (->> (for [lhs colseq]
           [(ds-col/column-name lhs)
            (->> colseq
                 (map (fn [rhs]
                        [(ds-col/column-name rhs)
                         (ds-col/correlation lhs rhs correlation-type)]))
                 (sort-by (comp #(Math/abs (double %)) second) >))])
         (into {}))))


(defn column-name->label-map
  [column-name options]
  (if-let [col-label-map (get-in options [:label-map column-name])]
    col-label-map
    (throw (ex-info (format "Failed to find label map for column %s"
                            column-name)
                    {:label-column column-name
                     :label-map-keys (keys (:label-map options))}))))


(defn options->label-map
  [{:keys [label-columns label-map] :as options}]
  (when-not (= 1 (count label-columns))
    (throw (ex-info (format "Multiple label columns found: %s" label-columns)
                    {:label-columns label-columns})))
  (column-name->label-map (first label-columns) options))


(defn options->label-inverse-map
  "Given options generated during ETL operations and annotated with :label-columns
  sequence container 1 label column, generate a reverse map that maps from a dataset
  value back to the label that generated that value."
  [options]
  (c-set/map-invert (options->label-map options)))


(defn options->num-classes
  "Given a dataset and correctly built options from pipeline operations,
  return the number of classes used for the label.  Error if not classification
  dataset."
  ^long [options]
  (count (options->label-map options)))


(defn options->feature-ecount
  "When columns aren't scalars then this will change.
  For now, just the number of feature columns."
  ^long [options]
  (count (:feature-columns options)))


(defn options->model-type
  "Check the label column after dataset processing.
  Return either
  :regression
  :classification"
  [{:keys [label-columns] :as options}]
  (if (or (not= 1 (count label-columns))
          (nil? (get-in options [:label-map (first label-columns)])))
    :regression
    :classification))

(defn- is-one-hot-label-map?
  [label-map]
  (let [[col-val col-entry] (first label-map)]
    (not (number? col-entry))))


(defn- inverse-map-one-hot-column-values-fn
  [src-column column-label-map]
  (let [inverse-map (c-set/map-invert column-label-map)
        colname-seq (->> inverse-map
                         keys
                         (map first)
                         distinct)]
    (fn [col-idx col-values]
      (let [nonzero-entries
            (->> (map (fn [col-name col-val]
                        (when-not (= 0 (long col-val))
                          [col-name (long col-val)]))
                      colname-seq col-values)
                 (remove nil?))]
        (when-not (= 1 (count nonzero-entries))
          (throw (ex-info
                  (format "Multiple (or zero) nonzero entries detected:[%s]%s"
                          col-idx nonzero-entries)
                  {:column-name src-column
                   :label-map column-label-map})))
        (if-let [colval (get inverse-map (first nonzero-entries))]
          colval
          (throw (ex-info (format "Failed to find column entry %s: %s"
                                  (first nonzero-entries)
                                  (keys inverse-map))
                          {:entry-label (first nonzero-entries)
                           :label-map column-label-map})))))))


(defn- inverse-map-one-hot-columns
  [dataset src-column column-label-map]
  (let [colname-seq (->> column-label-map
                         vals
                         (map first)
                         distinct)]
    (->> (select dataset colname-seq :all)
         index-value-seq
         (map (inverse-map-one-hot-column-values-fn column-label-map)))))


(defn- inverse-map-string->number-col-fn
  [src-column column-label-map]
  (let [inverse-map (c-set/map-invert column-label-map)]
    (fn [col-val]
      (if-let [col-label (get inverse-map (long col-val))]
        col-label
        (throw (ex-info
                (format "Failed to find label for column value %s"
                        col-val)
                {:inverse-label-map inverse-map}))))))


(defn- inverse-map-string->number-columns
  [dataset src-column column-label-map]
  (let [column-values (-> (column dataset src-column)
                          ds-col/column-values)
        inverse-map (c-set/map-invert column-label-map)]
    (->> column-values
         (mapv (inverse-map-string->number-col-fn src-column column-label-map)))))


(defn column-values->categorical
  "Given a column encoded via either string->number or one-hot, reverse
  map to the a sequence of the original string column values."
  [dataset src-column {:keys [label-map] :as options}]
  (when-not (contains? label-map src-column)
    (throw (ex-info (format "Failed to find column %s in label map %s"
                            src-column (keys label-map))
                    {:column-name src-column
                     :label-map label-map})))

  (let [label-map (get label-map src-column)]
    (if (is-one-hot-label-map? label-map)
      (inverse-map-one-hot-columns dataset src-column label-map)
      (inverse-map-string->number-columns dataset src-column label-map))))


(defn ->flyweight
  "Convert dataset to seq-of-maps dataset.  Flag indicates if errors should be thrown on
  missing values or if nil should be inserted in the map.  IF a label map is passed in
  then for the columns that are present in the label map a reverse mapping is done such
  that the flyweight maps contain the labels and not their encoded values."
  [dataset & {:keys [column-name-seq
                     error-on-missing-values?
                     label-map]
              :or {column-name-seq :all
                   error-on-missing-values? true}}]
  (let [dataset (select dataset column-name-seq :all)
        column-name-seq (map ds-col/column-name (columns dataset))
        inverse-label-map (->> label-map
                               (map (juxt first (comp c-set/map-invert second)))
                               (into {}))
        retval
        (if error-on-missing-values?
          (ds-map dataset (fn [& args]
                            (zipmap column-name-seq args)))
          ;;Much slower algorithm
          (if-let [ds-columns (seq (columns dataset))]
            (let [ecount (long (apply min (map dtype/ecount ds-columns)))
                  columns (columns dataset)]
              (for [idx (range ecount)]
                (->> (for [col columns]
                       [(ds-col/column-name col)
                        (when-not (ds-col/is-missing? col idx)
                          (ds-col/get-column-value col idx))])
                     (remove nil?)
                     (into {}))))))]
    (if (seq inverse-label-map)
      (->>
       retval
       (map
        #(->> inverse-label-map
              (reduce
               (fn [row-map [colname value-map]]
                 (if (contains? row-map colname)
                   (if-let [mapped-val
                            (get value-map (long (get row-map colname)))]
                     (assoc row-map colname mapped-val)
                     (throw
                      (ex-info
                       (format "Failed to find column %s value %s in label map %s"
                               colname (get row-map colname) value-map) {})))
                   row-map))
               %))))
      retval)))


(declare ->dataset)


(defn ->k-fold-datasets
  "Given 1 dataset, prepary K datasets using the k-fold algorithm.
  Randomize dataset defaults to true which will realize the entire dataset
  so use with care if you have large datasets."
  [dataset k {:keys [randomize-dataset?]
              :or {randomize-dataset? true}
              :as options}]
  (let [dataset (->dataset dataset options)
        [n-cols n-rows] (m/shape dataset)
        indexes (cond-> (range n-rows)
                  randomize-dataset? shuffle)
        fold-size (inc (quot (long n-rows) k))
        folds (vec (partition-all fold-size indexes))]
    (for [i (range k)]
      {:test-ds (select dataset :all (nth folds i))
       :train-ds (select dataset :all (->> (keep-indexed #(if (not= %1 i) %2) folds)
                                           (apply concat )))})))


(defn ->train-test-split
  [dataset {:keys [randomize-dataset? train-fraction]
            :or {randomize-dataset? true
                 train-fraction 0.7}
            :as options}]
  (let [dataset (->dataset dataset options)
        [n-cols n-rows] (m/shape dataset)
        indexes (cond-> (range n-rows)
                  randomize-dataset? shuffle)
        n-elems (long n-rows)
        n-training (long (Math/round (* n-elems (double train-fraction))))]
    {:train-ds (select dataset :all (take n-training indexes))
     :test-ds (select dataset :all (drop n-training indexes))}))


(defn ->row-major
  "Given a dataset and a map if desired key names to sequences of columns,
  produce a sequence of maps where each key name points to contiguous vector
  composed of the column values concatenated.
  If colname-seq-map is not provided then each row defaults to
  {:features [feature-columns]
   :label [label-columns]}"
  ([dataset key-colname-seq-map {:keys [datatype]
                                 :or {datatype :float64}}]
   (let [key-val-seq (seq key-colname-seq-map)
         all-col-names (mapcat second key-val-seq)
         item-col-count-map (->> key-val-seq
                                 (map (fn [[item-k item-col-seq]]
                                        (when (seq item-col-seq)
                                          [item-k (count item-col-seq)])))
                                 (remove nil?)
                                 vec)]
     (ds-map dataset
             (fn [& column-values]
               (->> item-col-count-map
                    (reduce (fn [[flyweight column-values] [item-key item-count]]
                              (let [contiguous-array (dtype/make-array-of-type
                                                      datatype (take item-count
                                                                     column-values))]
                                (when-not (= (dtype/ecount contiguous-array)
                                             (long item-count))
                                  (throw
                                   (ex-info "Failed to get correct number of items"
                                            {:item-key item-key})))
                                [(assoc flyweight item-key contiguous-array)
                                 (drop item-count column-values)]))
                            [{} column-values])
                    first))
             all-col-names)))
  ([dataset options]
   (->row-major dataset (merge {:features (get options :feature-columns)}
                               (when (seq (get options :label-columns))
                                 {:label (get options :label-columns)}))
                options)))


(defn labels
  "Given a dataset and an options map, generate a sequence of labels.
  If label count is 1, then if there is a label-map associated with column
  generate sequence of labels."
  [dataset {:keys [label-columns label-map] :as options}]
  (when-not label-columns
    (throw (ex-info "No label columns indicated" {})))
  (if-let [label-column (when (= (count label-columns) 1)
                          (first label-columns))]
    (if (contains? label-map label-column)
      (column-values->categorical dataset label-column options)
      (-> (column dataset label-column)
          ds-col/column-values))
    (->> (->row-major dataset {:labels label-columns} options)
         (map :labels))))


(defn map-seq->dataset
  "Given a sequence of maps, construct a dataset.  Defaults to a tablesaw-based
  dataset."
  [map-seq {:keys [scan-depth
                   column-definitions
                   table-name
                   dataset-constructor]
            :or {scan-depth 100
                 table-name "_unnamed"
                 dataset-constructor 'tech.libs.tablesaw/map-seq->tablesaw-dataset}
            :as options}]
  ((parallel/require-resolve dataset-constructor)
   map-seq options))


(defn ->dataset
  ([dataset {:keys [table-name]
             :or {table-name "_unnamed"}
             :as options}]
   (if (satisfies? ds-proto/PColumnarDataset dataset)
     dataset
     (if (and (sequential? dataset)
              (or (not (seq dataset))
                  (map? (first dataset))))
       (map-seq->dataset dataset options)
       (throw (ex-info "Dataset appears to be empty or not convertible to a dataset"
                       {:dataset dataset})))))
  ([dataset]
   (->dataset dataset {})))



(defn to-column-major-double-array-of-arrays
  "Convert a dataset to a row major array of arrays.
  Note that if error-on-missing is false, missing values will appear as NAN."
  ^"[[D" [dataset & [error-on-missing?]]
  (into-array (Class/forName "[D")
              (->> (columns dataset)
                   (map #(ds-col/to-double-array % error-on-missing?)))))


(defn transpose-double-array-of-arrays
  ^"[[D" [^"[[D" input-data]
  (let [[n-cols n-rows] (m/shape input-data)
        ^"[[D" retval (into-array (repeatedly n-rows #(double-array n-cols)))
        n-cols (int n-cols)
        n-rows (int n-rows)]
    (parallel/parallel-for
     row-idx
     n-rows
     (let [^doubles target-row (aget retval row-idx)]
       (c-for [col-idx (int 0) (< col-idx n-cols) (inc col-idx)]
              (aset target-row col-idx (aget ^doubles (aget input-data col-idx)
                                             row-idx)))))
    retval))


(defn to-row-major-double-array-of-arrays
    "Convert a dataset to a column major array of arrays.
  Note that if error-on-missing is false, missing values will appear as NAN."
  ^"[[D" [dataset & [error-on-missing?]]
  (-> (to-column-major-double-array-of-arrays dataset error-on-missing?)
      transpose-double-array-of-arrays))


(defn k-means
  "Nan-aware k-means.
  Returns array of centroids in row-major array-of-array-of-doubles format."
  ^"[[D" [dataset & [k max-iterations num-runs error-on-missing?]]
  ;;Smile expects data in row-major format.  If we use ds/->row-major, then NAN
  ;;values will throw exceptions and it won't be as efficient as if we build the
  ;;datastructure with a-priori knowledge
  (let [num-runs (int (or num-runs 1))]
    (if (= num-runs 1)
      (-> (KMeans/lloyd (to-row-major-double-array-of-arrays dataset error-on-missing?)
                        (int (or k 5))
                        (int (or max-iterations 100)))
          (.centroids))
      (-> (KMeans. (to-row-major-double-array-of-arrays dataset error-on-missing?)
                   (int (or k 5))
                   (int (or max-iterations 100))
                   (int num-runs))
          (.centroids)))))


(defn- ensure-no-missing!
  [dataset msg-begin]
  (when-let [cols-miss (columns-with-missing-seq dataset)]
    (throw (ex-info msg-begin
                    {:missing-columns cols-miss}))))


(defn g-means
  "g-means. Not NAN aware, missing is an error.
  Returns array of centroids in row-major array-of-array-of-doubles format."
  ^"[[D" [dataset & [max-k error-on-missing?]]
  ;;Smile expects data in row-major format.  If we use ds/->row-major, then NAN
  ;;values will throw exceptions and it won't be as efficient as if we build the
  ;;datastructure with a-priori knowledge
  (ensure-no-missing! dataset "G-Means - dataset cannot have missing values")
  (-> (GMeans. (to-row-major-double-array-of-arrays dataset error-on-missing?)
               (int (or max-k 5)))
      (.centroids)))


(defn x-means
  "x-means. Not NAN aware, missing is an error.
  Returns array of centroids in row-major array-of-array-of-doubles format."
  ^"[[D" [dataset & [max-k error-on-missing?]]
  ;;Smile expects data in row-major format.  If we use ds/->row-major, then NAN
  ;;values will throw exceptions and it won't be as efficient as if we build the
  ;;datastructure with a-priori knowledge
  (ensure-no-missing! dataset "X-Means - dataset cannot have missing values")
  (-> (XMeans. (to-row-major-double-array-of-arrays dataset error-on-missing?)
               (int (or max-k 5)))
      (.centroids)))


(def find-static
  (parallel/memoize
   (fn [^Class cls ^String fn-name & fn-arg-types]
     (let [method (doto (.getDeclaredMethod cls fn-name (into-array ^Class fn-arg-types))
                    (.setAccessible true))]
       (fn [& args]
         (.invoke method nil (into-array ^Object args)))))))


(defn nan-aware-mean
  ^double [^doubles col-data]
  (let [col-len (alength col-data)]
    (let [[sum n-elems]
          (loop [sum (double 0)
                 n-elems (int 0)
                 idx (int 0)]
            (if (< idx col-len)
              (let [col-val (aget col-data (int idx))]
                (if-not (Double/isNaN col-val)
                  (recur (+ sum col-val)
                         (unchecked-add n-elems 1)
                         (unchecked-add idx 1))
                  (recur sum
                         n-elems
                         (unchecked-add idx 1))))
              [sum n-elems]))]
      (if-not (= 0 (long n-elems))
        (/ sum (double n-elems))
        Double/NaN))))


(defn nan-aware-squared-distance
  "Nan away squared distance."
  ^double [lhs rhs]
  ;;Wrap find-static so we have good type hinting.
  ((find-static PartitionClustering "squaredDistance"
                (Class/forName "[D")
                (Class/forName "[D"))
   lhs rhs))


(defn group-rows-by-nearest-centroid
  [dataset ^"[[D" row-major-centroids & [error-on-missing?]]
  (let [[num-centroids num-columns] (m/shape row-major-centroids)
        [ds-cols ds-rows] (m/shape dataset)
        num-centroids (int num-centroids)
        num-columns (int num-columns)
        ds-cols (int ds-cols)
        ds-rows (int ds-rows)]

    (when-not (= num-columns ds-cols)
      (throw (ex-info (format "Centroid/Dataset column count mismatch - %s vs %s"
                              num-columns ds-cols)
                      {:centroid-num-cols num-columns
                       :dataset-num-cols ds-cols})))

    (when (= 0 num-centroids)
      (throw (ex-info "No centroids passed in."
                      {:centroid-shape (m/shape row-major-centroids)})))

    (->> (to-row-major-double-array-of-arrays dataset error-on-missing?)
         (map-indexed vector)
         (pmap (fn [[row-idx row-data]]
                 {:row-idx row-idx
                  :row-data row-data
                  :centroid-idx
                  (loop [current-idx (int 0)
                         best-distance (double 0.0)
                         best-idx (int 0)]
                    (if (< current-idx num-centroids)
                      (let [new-distance (nan-aware-squared-distance
                                          (aget row-major-centroids current-idx)
                                          row-data)]
                        (if (or (= current-idx 0)
                                (< new-distance best-distance))
                          (recur (unchecked-add current-idx 1)
                                 new-distance
                                 current-idx)
                          (recur (unchecked-add current-idx 1)
                                 best-distance
                                 best-idx)))
                      best-idx))}))
         (group-by :centroid-idx))))


(defn compute-centroid-and-global-means
  "Return a map of:
  centroid-means - centroid-index -> (double array) column means.
  global-means - global means (double array) for the dataset."
  [dataset ^"[[D" row-major-centroids]
  {:centroid-means
   (->> (group-rows-by-nearest-centroid dataset row-major-centroids false)
        (map (fn [[centroid-idx grouping]]
               [centroid-idx (->> (map :row-data grouping)
                                  (into-array (Class/forName "[D"))
                                  ;;Make column major
                                  transpose-double-array-of-arrays
                                  (pmap nan-aware-mean)
                                  double-array)]))
        (into {}))
   :global-means (->> (columns dataset)
                      (pmap (comp nan-aware-mean
                                  #(ds-col/to-double-array % false)))
                      double-array)})


(defn- non-nan-column-mean
  "Return the column mean, if it exists in the groupings else return nan."
  [centroid-groupings centroid-means row-idx col-idx]
  (let [applicable-means (->> centroid-groupings
                              (filter #(contains? (:row-indexes %) row-idx))
                              seq)]
    (when-not (< (count applicable-means) 2)
      (throw (ex-info "Programmer Error...Multiple applicable means seem to apply"
                      {:applicable-mean-count (count applicable-means)
                       :row-idx row-idx})))
    (when-let [{:keys [centroid-idx]} (first applicable-means)]
      (when-let [centroid-means (get centroid-means centroid-idx)]
        (let [col-mean (aget ^doubles centroid-means (int col-idx))]
          (when-not (Double/isNaN col-mean)
            col-mean))))))


(defn impute-missing-by-centroid-averages
  "Impute missing columns by first grouping by nearest centroids and then computing the
  mean.  In the case where the grouping for a given centroid contains all NaN's, use the
  global dataset mean.  In the case where this is NaN, this algorithm will fail to
  replace the missing values with meaningful values.  Return a new dataset."
  [dataset row-major-centroids {:keys [centroid-means global-means]}]
  (let [columns-with-missing (->> (columns dataset)
                                  (map-indexed vector)
                                  ;;For the columns that actually have something missing
                                  ;;that we care about...
                                  (filter #(> (count (ds-col/missing (second %)))
                                              0)))]
    (if-not (seq columns-with-missing)
      dataset
      (let [;;Partition data based on all possible columns
            centroid-groupings
            (->> (group-rows-by-nearest-centroid dataset row-major-centroids false)
                 (mapv (fn [[centroid-idx grouping]]
                         {:centroid-idx centroid-idx
                          :row-indexes (set (map :row-idx grouping))})))
            [n-cols n-rows] (m/shape dataset)
            n-rows (int n-rows)
            ^doubles global-means global-means]
        (->> columns-with-missing
             (reduce (fn [dataset [col-idx source-column]]
                       (let [col-idx (int col-idx)]
                         (update-column
                          dataset (ds-col/column-name source-column)
                          (fn [old-column]
                            (let [src-doubles (ds-col/to-double-array old-column false)
                                  new-col (ds-col/new-column
                                           old-column :float64
                                           (m/ecount old-column)
                                           (ds-col/metadata old-column))
                                  ^doubles col-doubles (dtype/->array new-col)]
                              (parallel/parallel-for
                               row-idx
                               n-rows
                               (if (Double/isNaN (aget src-doubles row-idx))
                                 (aset col-doubles row-idx
                                       (double
                                        (or (non-nan-column-mean centroid-groupings
                                                                 centroid-means
                                                                 row-idx col-idx)
                                            (aget global-means col-idx))))
                                 (aset col-doubles row-idx (aget src-doubles row-idx))))
                              new-col)))))
                     dataset))))))
