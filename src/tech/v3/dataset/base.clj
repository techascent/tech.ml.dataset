(ns ^:no-doc tech.v3.dataset.base
  "Base dataset bare bones implementation.  Methods here are used in further
  implementations and they are exposed to users."
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.argops :as argops]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.bitmap :refer [->bitmap] :as bitmap]
            [tech.v3.datatype.packing :as packing]
            [tech.v3.datatype.copy-make-container :as dtype-cmc]
            [tech.v3.datatype.list :as dtype-list]
            [tech.v3.datatype.array-buffer :as array-buffer]
            [tech.v3.parallel.for :as pfor]
            [tech.v3.dataset.column :as ds-col]
            [tech.v3.protocols.dataset :as ds-proto]
            [tech.v3.dataset.impl.dataset :as ds-impl]
            [tech.v3.dataset.string-table :as str-table]
            [tech.v3.dataset.readers :as ds-readers]
            [tech.v3.dataset.dynamic-int-list :as dyn-int-list]
            [primitive-math :as pmath])
  (:import [java.io InputStream File]
           [tech.v3.datatype Buffer ObjectReader PrimitiveList]
           [tech.v3.dataset.impl.dataset Dataset]
           [tech.v3.dataset.impl.column Column]
           [tech.v3.dataset.string_table StringTable]
           [java.util List HashSet LinkedHashMap Map Arrays HashMap
            ArrayList]
           [org.roaringbitmap RoaringBitmap]
           [clojure.lang IFn]
           [smile.data DataFrame]
           [smile.io Read])
  (:refer-clojure :exclude [filter group-by sort-by concat take-nth]))


(set! *warn-on-reflection* true)


(defn dataset-name
  [dataset]
  (ds-proto/dataset-name dataset))

(defn set-dataset-name
  [dataset ds-name]
  (ds-proto/set-dataset-name dataset ds-name))

(defn row-count
  ^long [dataset]
  (second (dtype/shape dataset)))

(defn ds-row-count
  [dataset]
  (row-count dataset))

(defn column-count
  ^long [dataset]
  (first (dtype/shape dataset)))

(defn ds-column-count
  [dataset]
  (column-count dataset))


(defn columns
  "Return sequence of all columns in dataset."
  [dataset]
  (ds-proto/columns dataset))


(defn column
  [dataset colname]
  (if-let [retval (get dataset colname)]
    retval
    (errors/throwf "Unable to find column %s" colname)))


(defn column-names
  "In-order sequence of column names"
  [dataset]
  (->> (ds-proto/columns dataset)
       (map ds-col/column-name)))


(defn has-column?
  [dataset column-name]
  (contains? dataset column-name))


(defn columns-with-missing-seq
  "Return a sequence of:
```clojure
  {:column-name column-name
   :missing-count missing-count
  }
```
  or nil of no columns are missing data."
  [dataset]
  (->> (columns dataset)
       (map (fn [col]
              (let [missing-count (dtype/ecount (ds-col/missing col))]
                (when-not (= 0 missing-count)
                  {:column-name (ds-col/column-name col)
                   :missing-count missing-count}))))
       (remove nil?)
       seq))


(defn add-column
  "Add a new column. Error if name collision"
  [dataset column]
  (ds-proto/add-column dataset column))


(defn new-column
  "Create a new column from some values"
  [dataset column-name values]
  (->> (if (ds-col/is-column? values)
         (ds-col/set-name values column-name)
         (ds-col/new-column column-name values))
       (add-column dataset)))


(defn remove-column
  "Fails quietly"
  [dataset col-name]
  (ds-proto/remove-column dataset col-name))


(defn remove-columns
  "Same as drop-columns"
  [dataset colname-seq]
  (reduce ds-proto/remove-column dataset colname-seq))


(defn drop-columns
  "Same as remove-columns"
  [dataset col-name-seq]
  (remove-columns dataset col-name-seq))


(defn update-column
  "Update a column returning a new dataset.  update-fn is a column->column
  transformation.  Error if column does not exist."
  [dataset col-name update-fn]
  (ds-proto/update-column dataset col-name update-fn))


(defn order-column-names
  "Order a sequence of columns names so they match the order in the
  original dataset.  Missing columns are placed last."
  [dataset colname-seq]
  (let [colname-set (set colname-seq)
        ordered-columns (->> (columns dataset)
                             (map ds-col/column-name)
                             (clojure.core/filter colname-set))]
    (clojure.core/concat ordered-columns
                         (remove (set ordered-columns) colname-seq))))


(defn update-columns
  "Update a sequence of columns."
  [dataset column-name-seq update-fn]
  (reduce (fn [dataset colname]
            (update-column dataset colname update-fn))
          dataset
          column-name-seq))


(defn add-or-update-column
  "If column exists, replace.  Else append new column."
  ([dataset colname column]
   (ds-proto/add-or-update-column dataset colname column))
  ([dataset column]
   (add-or-update-column dataset (ds-col/column-name column) column)))


(defn select
  "Reorder/trim dataset according to this sequence of indexes.  Returns a new dataset.
  colname-seq - one of:
    - :all - all the columns
    - sequence of column names - those columns in that order.
    - implementation of java.util.Map - column order is dictate by map iteration order
       selected columns are subsequently named after the corresponding value in the map.
       similar to `rename-columns` except this trims the result to be only the columns
       in the map.
  index-seq - either keyword :all or list of indexes.  May contain duplicates.
  "
  [dataset colname-seq index-seq]
  (let [index-seq (if (number? index-seq)
                    [index-seq]
                    index-seq)]
    (ds-proto/select dataset colname-seq index-seq)))


(defn unordered-select
  "Perform a selection but use the order of the columns in the existing table; do
  *not* reorder the columns based on colname-seq.  Useful when doing selection based
  on sets or persistent hash maps."
  [dataset colname-seq index-seq]
  (let [colname-seq (cond
                      (instance? Map colname-seq)
                      (->> (column-names dataset)
                           (map (fn [colname]
                                  (when-let [cn-seq (get colname-seq colname)]
                                    [colname cn-seq])))
                           (remove nil?)
                           (reduce (fn [^Map item [k v]]
                                     (.put item k v)
                                     item)
                                   (LinkedHashMap.)))
                      (= :all colname-seq)
                      colname-seq
                      :else
                      (order-column-names dataset colname-seq))]
    (select dataset colname-seq index-seq)))


(defn select-columns
  [dataset col-name-seq]
  (select dataset col-name-seq :all))


(defn rename-columns
  "Rename columns using a map.  Does not reorder columns."
  [dataset colname-map]
  (->> (ds-proto/columns dataset)
       (map (fn [col]
              (let [old-name (ds-col/column-name col)]
                (if (contains? colname-map old-name)
                  (ds-col/set-name col (get colname-map old-name))
                  col))))
       (ds-impl/new-dataset (dataset-name dataset)
                            (meta dataset))))


(defn select-rows
  [dataset row-indexes]
  (select dataset :all row-indexes))


(defn drop-rows
  "Same as remove-rows."
  [dataset row-indexes]
  (let [n-rows (row-count dataset)
        row-indexes (if (dtype/reader? row-indexes)
                      row-indexes
                      (take n-rows row-indexes))]
    (if (== 0 (dtype/ecount row-indexes))
      dataset
      (select dataset :all
              (dtype-proto/set-and-not
               (bitmap/->bitmap (range n-rows))
               row-indexes)))))


(defn remove-rows
  "Same as drop-rows."
  [dataset row-indexes]
  (drop-rows dataset row-indexes))


(defn missing
  [dataset]
  (reduce #(dtype-proto/set-or %1 (ds-col/missing %2))
          (->bitmap)
          (vals dataset)))


(defn supported-column-stats
  "Return the set of natively supported stats for the dataset.  This must be at least
#{:mean :variance :median :skew}."
  [dataset]
  (ds-proto/supported-column-stats dataset))


(defn check-dataset-wrong-position
  [item]
  (when (instance? Dataset item)
    (throw (Exception. "Dataset now must be passed as last option.
This is an interface change and we do apologize!"))))


(defn filter
  "dataset->dataset transformation.  Predicate is passed a map of
  colname->column-value."
  ([dataset predicate]
   (->> (ds-readers/mapseq-reader dataset)
        (argops/argfilter predicate)
        (select dataset :all))))


(defn filter-column
  "Filter a given column by a predicate.  Predicate is passed column values.
  If predicate is *not* an instance of Ifn it is treated as a value and will
  be used as if the predicate is #(= value %).
  Returns a dataset."
  [dataset colname predicate]
  (let [predicate (if (instance? IFn predicate)
                    predicate
                    (let [pred-dtype (dtype/get-datatype predicate)]
                      (cond
                        (casting/integer-type? pred-dtype)
                        (let [predicate (long predicate)]
                          (fn [^long arg] (== arg predicate)))
                        (casting/float-type? pred-dtype)
                        (let [predicate (double predicate)]
                          (fn [^double arg] (== arg predicate)))
                        :else
                        #(= predicate %))))]
    (->> (get dataset colname)
         (argops/argfilter predicate)
         (select dataset :all))))


(defn group-by->indexes
  ([dataset key-fn]
   (->> (ds-readers/mapseq-reader dataset)
        (argops/arggroup-by key-fn {:unordered? false}))))


(defn group-by
  "Produce a map of key-fn-value->dataset.  key-fn is a function taking
  a map of colname->column-value.  Selecting which columns are used in the key-fn
  using column-name-seq is optional but will greatly improve performance."
  ([dataset key-fn]
   (->> (group-by->indexes dataset key-fn)
        (pmap (fn [[k v]] [k (select dataset :all v)]))
        (into {}))))


(defn group-by-column->indexes
  [dataset colname]
  (->> (column dataset colname)
       (argops/arggroup {:unordered? false})))


(defn group-by-column
  "Return a map of column-value->dataset."
  [dataset colname]
  (->> (group-by-column->indexes dataset colname)
       (pmap (fn [[k v]]
               [k (-> (select dataset :all v)
                      (set-dataset-name k))]))
       (into {})))


(defn sort-by
  "Sort a dataset by a key-fn and compare-fn."
  ([dataset key-fn compare-fn]
   (->> (ds-readers/mapseq-reader dataset)
        (dtype/emap key-fn :object)
        (argops/argsort compare-fn)
        (select dataset :all)))
  ([dataset key-fn]
   (sort-by dataset key-fn nil)))


(defn sort-by-column
  "Sort a dataset by a given column using the given compare fn."
  ([dataset colname compare-fn]
   (->> (argops/argsort compare-fn (dataset colname))
        (select dataset :all)))
  ([dataset colname]
   (sort-by-column dataset colname nil)))


(defn- do-concat
  [reader-concat-fn dataset other-datasets]
  (let [datasets (->> (clojure.core/concat [dataset] (remove nil? other-datasets))
                      (remove nil?)
                      seq)]
    (if (== 1 (count datasets))
      (first datasets)
      (when-let [dataset (first datasets)]
        (let [column-list
              (->> datasets
                   (mapcat (fn [dataset]
                             (->> (columns dataset)
                                  (mapv (fn [col]
                                          (assoc (meta col)
                                                 :column
                                                 col
                                                 :table-name (dataset-name dataset)))))))
                   (clojure.core/group-by :name))
              label-map (->> datasets
                             (map (comp :label-map meta))
                             (apply merge))]
          (when-not (= 1 (count (->> (vals column-list)
                                     (map count)
                                     distinct)))
            (throw (ex-info "Dataset is missing a column" {})))
          (->> column-list
               (map (fn [[colname columns]]
                      (let [columns (map :column columns)
                            final-dtype (if (== 1 (count columns))
                                          (dtype/get-datatype (first columns))
                                          (reduce (fn [lhs-dtype rhs-dtype]
                                                    ;;Exact matches don't need to be widening/promotion
                                                    (if-not (= lhs-dtype rhs-dtype)
                                                      (casting/simple-operation-space
                                                       (packing/unpack lhs-dtype)
                                                       (packing/unpack rhs-dtype))
                                                      lhs-dtype))
                                                  (map dtype/get-datatype columns)))
                            column-values (reader-concat-fn final-dtype columns)
                            missing
                            (->> (reduce
                                  (fn [[missing offset] col]
                                    [(dtype-proto/set-or
                                      missing
                                      (dtype-proto/set-offset (ds-col/missing col)
                                                              offset))
                                     (+ offset (dtype/ecount col))])
                                  [(->bitmap) 0]
                                  columns)
                                 (first))
                            first-col (first columns)]
                        (ds-col/new-column colname
                                           column-values
                                           (meta first-col)
                                           missing))))
               (ds-impl/new-dataset (dataset-name dataset))
               (#(with-meta % {:label-map label-map}))))))))


(defn concat-inplace
  "Concatenate datasets in place.  Respects missing values.  Datasets must all have the
  same columns.  Result column datatypes will be a widening cast of the datatypes."
  [dataset & datasets]
  (do-concat #(dtype/concat-buffers %1 %2)
             dataset datasets))


(defn concat-copying
  "Concatenate datasets into a new dataset copying data.  Respects missing values.
  Datasets must all have the same columns.  Result column datatypes will be a widening
  cast of the datatypes."
  [dataset & datasets]
  (let [datasets (->> (clojure.core/concat [dataset] (remove nil? datasets))
                      (remove nil?)
                      seq)
        n-rows (long (reduce + (map row-count datasets)))]
    (do-concat #(-> (dtype/copy-raw->item! %2
                                           (dtype/make-container
                                            :jvm-heap %1 n-rows))
                    (first))
               (first datasets) (rest datasets))))


(defn concat
  "Concatenate datasets in place.  Respects missing values.  Datasets must all have the
  same columns.  Result column datatypes will be a widening cast of the datatypes.
  Also see concat-copying as this may be faster in many situations."
  [dataset & datasets]
  (apply concat-inplace dataset datasets))


(defn- sorted-int32-sequence
  [idx-seq]
  (let [data (dtype/->int-array idx-seq)]
    (Arrays/sort data)
    data))


(defn unique-by
  "Map-fn function gets passed map for each row, rows are grouped by the
  return value.  Keep-fn is used to decide the index to keep.

  :keep-fn - Function from key,idx-seq->idx.  Defaults to #(first %2)."
  ([dataset {:keys [keep-fn]
            :or {keep-fn #(first %2)}
             :as _options}
    map-fn]
   (->> (group-by->indexes dataset map-fn)
        (map (fn [[k v]] (keep-fn k v)))
        (sorted-int32-sequence)
        (select-rows dataset)))
  ([dataset map-fn]
   (unique-by dataset {} map-fn)))


(defn unique-by-column
  "Map-fn function gets passed map for each row, rows are grouped by the
  return value.  Keep-fn is used to decide the index to keep.

  :keep-fn - Function from key, idx-seq->idx.  Defaults to #(first %2)."
  ([dataset
    {:keys [keep-fn]
     :or {keep-fn #(first %2)}
     :as _options}
    colname]
   (->> (group-by-column->indexes dataset colname)
        (map (fn [[k v]] (keep-fn k v)))
        (sorted-int32-sequence)
        (select dataset :all)))
  ([dataset colname]
   (unique-by-column dataset {} colname)))


(defn take-nth
  [dataset n-val]
  (select dataset :all (->> (range (second (dtype/shape dataset)))
                            (clojure.core/take-nth n-val))))

(casting/add-object-datatype! :dataset Dataset)


(defn dataset->string
  ^String [ds]
  (.toString ^Object ds))


(defn ensure-array-backed
  "Ensure the column data in the dataset is stored in pure java arrays.  This is
  sometimes necessary for interop with other libraries and this operation will
  force any lazy computations to complete.  This also clears the missing set
  for each column and writes the missing values to the new arrays.

  Columns that are already array backed and that have no missing values are not
  changed and retuned.

  The postcondition is that dtype/->array will return a java array in the appropriate
  datatype for each column.

  options -
  :unpack? - unpack packed datetime types.  Defaults to true"
  ([ds {:keys [unpack?]
        :or {unpack? true}}]
   (reduce (fn [ds col]
             (let [colname (ds-col/column-name col)
                   col (if unpack?
                         (packing/unpack col)
                         col)]
               (assoc ds colname (dtype-cmc/->array col))))
           ds
           (columns ds)))
  ([ds]
   (ensure-array-backed ds {})))


(defn column->string-table
  ^StringTable [^Column col]
  (if-let [retval (when (instance? StringTable (.data col))
                    (.data col))]
    retval
    (throw (Exception. (format "Column %s does not contain a string table"
                               (ds-col/column-name col))))))


(defn ensure-column-string-table
  "Ensure this column is backed by a string table.
  If not, return a new column that is.
  Column must be :string datatype."
  ^StringTable [col]
  (when-not (= :string (dtype/get-datatype col))
    (throw (Exception.
            (format "Column %s does not have :string datatype"
                    (ds-col/column-name col)))))
  (if (not (instance? StringTable (.data ^Column col)))
    (str-table/string-table-from-strings col)
    (.data ^Column col)))


(defn ensure-dataset-string-tables
  "Given a dataset, ensure every string column is backed by a string table."
  [ds]
  (reduce
   (fn [ds col]
     (if (= :string (dtype/get-datatype col))
       (let [missing (ds-col/missing col)
             metadata (meta col)
             colname (:name metadata)
             str-t (ensure-column-string-table col)]
         (assoc ds (ds-col/column-name col)
                (ds-col/new-column colname str-t metadata missing)))
       ds))
   ds
   (vals ds)))


(defn- column->string-data
  [str-col]
  (let [coldata (.data ^Column str-col)
        ^StringTable str-table
        (if (instance? StringTable coldata)
          coldata
          (str-table/string-table-from-strings coldata))
        ^PrimitiveList str-data-buf (dtype/make-container :list :int8 0)
        ^PrimitiveList offset-buf (dtype/make-container :list :int32 0)
        data-ary (dtype-cmc/->array (.data str-table))]
    (pfor/consume!
     #(do
        (.addLong offset-buf (.lsize str-data-buf))
        (let [str-bytes (.getBytes ^String %)]
          (.addAll str-data-buf (dtype/->buffer str-bytes))))
     (.int->str str-table))
    ;;One extra long makes deserializing a bit less error prone.
    (.addLong offset-buf (.lsize str-data-buf))
    {:string-data (dtype/->array str-data-buf)
     :offsets (dtype/->array offset-buf)
     ;;Between version 1 and 2 we changed the string table to be just
     ;;an array of strings intead of a hashmap.
     ;;For version 3, we had to serialize string data into byte arrays
     ;;as java string serialization is considered a security risk
     :version 3
     :entries data-ary}))


(defn- string-data->column-data
  [{:keys [string-table entries version] :as entry}]
  (let [int-data (dyn-int-list/make-from-container entries)]
    (cond
      ;;Version
      (= version 3)
      (let [{:keys [string-data offsets]} entry
            string-data ^bytes string-data
            offsets (dtype/->buffer offsets)
            n-elems (dec (.lsize offsets))
            ^PrimitiveList int->str
            (->> (dtype/make-reader
                  :string n-elems
                  (let [start-off (.readInt offsets idx)
                        end-off (.readInt offsets (inc idx))]
                    (String. string-data start-off (pmath/- end-off start-off))))
                 (dtype/make-container :list :string))
            str->int (HashMap. (dtype/ecount int->str))]
        (dotimes [idx n-elems]
          (.put str->int (.get int->str idx) idx))
        (StringTable. int->str str->int int-data))
      (= version 2)
      (let [^List int->str (dtype-list/wrap-container string-table)
            str->int (HashMap. (dtype/ecount int->str))
            n-elems (.size int->str)]
        ;;build reverse map
        (dotimes [idx n-elems]
          (.put str->int (.get int->str idx) idx))
        (StringTable. int->str str->int int-data))
      :else
      (let [^Map str->int string-table
            int->str-ary-list (ArrayList. (count str->int))
            _ (doseq [[k v] str->int]
                (let [v (unchecked-int v)
                      list-size (.size int->str-ary-list)]
                  (dotimes [idx (max 0 (- (inc v) list-size))]
                    (.add int->str-ary-list 0))
                  (.set int->str-ary-list v k)))]
        (StringTable. int->str-ary-list str->int int-data)))))


(defn dataset->data
  "Convert a dataset to a pure clojure datastructure.  Returns a map with two keys:
  {:metadata :columns}.
  :columns is a vector of column definitions appropriate for passing directly back
  into new-dataset.
  A column definition in this case is a map of {:name :missing :data :metadata}."
  [ds]
  {:metadata (meta ds)
   :columns (->> (columns ds)
                 (mapv (fn [col]
                         ;;Only store packed data.  This can sidestep serialization issues
                         (let [col (packing/pack col)
                               dtype (dtype/get-datatype col)]
                           {:metadata (meta col)
                            :missing (bitmap/bitmap->efficient-random-access-reader (ds-col/missing col))
                            :name (:name (meta col))
                            :data (cond
                                    (= :string (dtype/get-datatype col))
                                    (column->string-data col)
                                    ;;Nippy doesn't handle object arrays or arraylists
                                    ;;very well.
                                    (= :object (casting/flatten-datatype dtype))
                                    (vec col)
                                    ;;Store the data as a jvm-native array
                                    :else
                                    (dtype-cmc/->array dtype col))}))))})


(defn data->dataset
  "Convert a data-ized dataset created via dataset->data back into a
  full dataset"
  [{:keys [metadata columns] :as input}]
  (->> columns
       (map (fn [{:keys [metadata] :as coldata}]
              (let [datatype (:datatype metadata)]
                (->
                 (cond
                   (= :string datatype)
                   (update coldata :data string-data->column-data)
                   (= :object (casting/flatten-datatype datatype))
                   (update coldata :data dtype/elemwise-cast datatype)
                   :else
                   (update coldata :data array-buffer/array-buffer datatype))
                 (clojure.core/assoc :force-datatype? true)
                 (clojure.core/update :missing bitmap/->bitmap)))))
       (ds-impl/new-dataset {:dataset-name (:name metadata)} metadata)))
