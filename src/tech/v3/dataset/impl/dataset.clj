(ns ^:no-doc tech.v3.dataset.impl.dataset
  (:require [tech.v3.protocols.column :as ds-col-proto]
            [tech.v3.protocols.dataset :as ds-proto]
            [tech.v3.dataset.impl.column :as col-impl]
            [tech.v3.dataset.print :as ds-print]
            [tech.v3.datatype.pprint :as dtype-pp]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.argtypes :as argtypes]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.bitmap :as bitmap]
            [tech.v3.dataset.impl.column-data-process :as column-data-process]
            [tech.v3.dataset.impl.column-base :as column-base]
            [tech.v3.datatype.graal-native :as graal-native])
  (:import [clojure.lang IPersistentMap IObj IFn Counted MapEntry]
           [java.util Map List LinkedHashSet]
           [tech.v3.datatype ObjectReader]))


(set! *warn-on-reflection* true)


(declare new-dataset map-entries empty-dataset)

;;ported from clojure.lang.APersistentMap
(defn- map-equiv [this o]
  (cond (not (instance? java.util.Map o)) false
        (and (instance? clojure.lang.IPersistentMap o)
             (not (instance? clojure.lang.MapEquivalence o))) false
        :else (let [^java.util.Map m o]
                (and (= (.size m) (count this))
                     (every? (fn [k]
                               (and (.containsKey m k)
                                    (= (this k) (.get m k)))) (keys this))))))


(defn- shorten-or-extend
  [^long n-rows reader]
  (let [reader-dtype (dtype/elemwise-datatype reader)
        reader-ecount (dtype/ecount reader)]
    (cond
      (== reader-ecount n-rows)
      reader
      (< reader-ecount n-rows)
      (dtype/concat-buffers (dtype/elemwise-datatype reader)
                            [reader (dtype/const-reader (column-base/datatype->missing-value
                                                         reader-dtype)
                                                        (- n-rows reader-ecount))])
      ;;Else the number of elements is greater than the number of rows
      :else
      (dtype/sub-buffer reader 0 n-rows))))


(defn- coldata->column
  [n-cols n-rows col-name new-col-data]
  (let [argtype (argtypes/arg-type new-col-data)
        n-rows (if (= 0 n-cols)
                 Integer/MAX_VALUE
                 n-rows)]
    (cond
      (ds-col-proto/is-column? new-col-data)
      (vary-meta new-col-data assoc :name col-name)
      ;;maps are scalars in dtype-land but you can pass in column data maps
      ;;so this next check is a bit hairy
      (and (identical? argtype :scalar)
           ;;This isn't a column data map.  Maps are
           (not (and (instance? Map new-col-data)
                     (.containsKey ^Map new-col-data :tech.v3.dataset/data))))
      (col-impl/new-column col-name (dtype/const-reader new-col-data n-rows))
      :else
      (let [map-data
            (if (or (= argtype :iterable)
                    (map? new-col-data))
              (column-data-process/prepare-column-data
               (if (map? new-col-data)
                 new-col-data
                 (take n-rows new-col-data)))
              ;;Else has to be reader or tensor.
              (let [data-ecount (dtype/ecount new-col-data)
                    new-col-data (if (> data-ecount n-rows)
                                   (dtype/sub-buffer new-col-data 0 n-rows)
                                   new-col-data)]
                (column-data-process/prepare-column-data new-col-data)))
            map-data (-> (update map-data :tech.v3.dataset/data
                                 (fn [data]
                                   (if-not (= 0 n-cols)
                                     (shorten-or-extend n-rows data)
                                     data)))
                         (assoc :tech.v3.dataset/name col-name))]
        (col-impl/new-column map-data)))))


(defn- nearest-range-start
  ^long [^long bound ^long range-start ^long increment]
  (if (> increment 0)
    ;;if starting before bound
    (if (>= range-start bound)
      range-start
      (+ range-start
         (+ increment
            (* increment
               (quot (- bound (inc range-start))
                     increment)))))
    ;;if starting after bound
    (if (<= range-start bound)
      range-start
      (+ range-start
         (+ increment
            (* increment
               (quot (- bound range-start)
                     increment)))))))


(defn- map-entries
  ^List [^List columns]
  (reify ObjectReader
    (lsize [rdr] (.size columns))
    (readObject [rdr idx]
      (let [col (.get columns idx)]
        (MapEntry. (:name (meta col)) col)))))



(deftype Dataset [^List columns
                  colmap
                  ^IPersistentMap metadata
                  ^{:unsynchronized-mutable true :tag 'int}  _hash
                  ^{:unsynchronized-mutable true :tag 'int}  _hasheq]
  java.util.Map
  (size [this]    (.count this))
  (isEmpty [this] (not (pos? (.count this))))
  (containsValue [_this v] (some #(= % v) columns))
  (get [this k] (.valAt this k))
  (put [_this _k _v]  (throw (UnsupportedOperationException.)))
  (remove [_this _k] (throw (UnsupportedOperationException.)))
  (putAll [_this _m] (throw (UnsupportedOperationException.)))
  (clear [_this]    (throw (UnsupportedOperationException.)))
  (keySet [_this]
    (let [retval (LinkedHashSet.)]
      (.addAll retval (map-entries columns))
      retval))
  (values [_this] columns)
  (entrySet [_this]
    (let [retval (LinkedHashSet.)]
      (.addAll retval (map #(clojure.lang.MapEntry. (:name (meta %)) %)
                           columns))
      retval))

  clojure.lang.ILookup
  (valAt [_this k]
    (when-let [idx (colmap k)]
      (.get columns idx)))
  (valAt [this k not-found]
    (if-let [res (.valAt this k)]
      res
      not-found))

  clojure.lang.MapEquivalence

  clojure.lang.IPersistentMap
  (assoc   [this k v] (.add-or-update-column this k v))
  (assocEx [this k v]
    (if-not (colmap k)
      (.add-or-update-column this k v)
      (throw (ex-info "Key already present" {:k k}))))
  ;;without implements (dissoc pm k) behavior
  (without [this k] (.remove-column this k))
  (entryAt [this k]
    (when-let [v (.valAt this k)]
      (clojure.lang.MapEntry. k v)))
  ;;No idea if this is correct behavior....
  (empty [_this] (empty-dataset))
  ;;ported from clojure java impl.
  (cons [this e]
    (cond (instance? java.util.Map$Entry e)
            (.assoc this (key e) (val e))
          (vector? e)
            (let [^clojure.lang.PersistentVector e e]
              (when-not (== (.count e) 2)
                (throw (ex-info "Vector arg to map conj must be a pair" {})))
              (.assoc this (.nth e 0) (.nth e 1)))
          :else
            (reduce (fn [^clojure.lang.Associative acc entry]
                      (.assoc acc (key entry) (val entry))) this e)))

  (containsKey [_this k] (.containsKey ^Map colmap k))

  ;;MAJOR DEVIATION
  ;;This conforms to clojure's idiom and projects the dataset onto a
  ;;seq of [column-name column] entries.  Legacy implementation defaulted
  ;;to using iterable, which was a seq of column.
  (seq [_this]
    ;;Do not reorder column data if possible.
    (when (pos? (count columns))
      (map #(clojure.lang.MapEntry. (:name (meta %)) %)  columns)))

  ;;Equality is likely a rat's nest, although we should be able to do it
  ;;if we wanted to!
  (hashCode [this]
    (when (== _hash (int -1))
      (set! _hash (clojure.lang.APersistentMap/mapHash  this)))
    _hash)

  clojure.lang.IHashEq
  ;;intentionally using seq instead of iterator for now.
  (hasheq [this]
    (when (== _hasheq (int -1))
      (set! _hasheq (hash-unordered-coll (or (.seq this)
                                             []))))
    _hasheq)

  ;;DOUBLE CHECK equals/equiv semantics...
  (equals [this o] (or (identical? this o)
                       (clojure.lang.APersistentMap/mapEquals this o)))

  (equiv [this o] (or (identical? this o)
                      (map-equiv this o)))

  ds-proto/PColumnarDataset
  (dataset-name [_dataset] (:name metadata))
  (set-dataset-name [_dataset new-name]
    (Dataset. columns colmap
     (assoc metadata :name new-name) _hash _hasheq))

  (columns [_dataset] columns)

  (add-column [dataset col]
    (let [existing-names (set (map ds-col-proto/column-name columns))
          new-col-name (ds-col-proto/column-name col)]
      (when (existing-names new-col-name)
        (throw (ex-info (format "Column of same name (%s) already exists in columns"
                                new-col-name)
                        {:existing-columns existing-names
                         :column-name new-col-name})))
      (new-dataset
       (ds-proto/dataset-name dataset)
       metadata
       (concat (ds-proto/columns dataset) [col]))))

  (remove-column [dataset col-name]
    (->> columns
         (remove #(= (ds-col-proto/column-name %) col-name))
         (new-dataset (ds-proto/dataset-name dataset) metadata)))

  (update-column [dataset col-name col-fn]
    (when-not (contains? colmap col-name)
      (throw (ex-info (format "Failed to find column %s" col-name)
                      {:col-name col-name
                       :col-names (keys colmap)})))
    (let [col-idx (get colmap col-name)
          col (.get columns (int col-idx))
          n-rows (long (second (dtype/shape dataset)))
          n-cols (long (first (dtype/shape dataset)))
          new-col-data (coldata->column n-cols n-rows col-name (col-fn col))]
      (Dataset.
       (assoc columns col-idx new-col-data)
       colmap
       metadata
       -1
       -1)))

  (add-or-update-column [dataset col-name new-col-data]
    (let [n-rows (long (second (dtype/shape dataset)))
          n-cols (long (first (dtype/shape dataset)))
          col-data (coldata->column n-cols n-rows col-name new-col-data)]
      (if (contains? colmap col-name)
        (ds-proto/update-column dataset col-name (constantly col-data))
        (ds-proto/add-column dataset col-data))))

  (select [dataset column-name-seq-or-map index-seq]
    ;;Conversion to a reader is expensive in some cases so do it here
    ;;to avoid each column doing it.
    (let [map-selector? (instance? Map column-name-seq-or-map)
          n-rows (long (second (dtype/shape dataset)))
          indexes (cond
                    (nil? index-seq)
                    []
                    (= :all index-seq)
                    nil
                    (dtype-proto/convertible-to-bitmap? index-seq)
                    (let [bmp (dtype-proto/as-roaring-bitmap index-seq)]
                      (dtype/->reader
                       (bitmap/bitmap->efficient-random-access-reader
                        bmp)))
                    (dtype-proto/convertible-to-range? index-seq)
                    (let [idx-seq (dtype-proto/->range index-seq {})
                          rstart (long (dtype-proto/range-start idx-seq))
                          rinc (long (dtype-proto/range-increment idx-seq))
                          rend (+ rstart (* rinc (dtype/ecount idx-seq)))]
                      (if (> rinc 0)
                        (range (nearest-range-start 0 rstart rinc)
                               (min n-rows rend)
                               rinc)
                        (range (nearest-range-start (dec n-rows) rstart rinc)
                               (max -1 rend)
                               rinc)))
                    (dtype/reader? index-seq)
                    (dtype/->reader index-seq)
                    :else
                    (dtype/->reader (dtype/make-container
                                     :jvm-heap :int32
                                     index-seq)))
          columns
          (cond
            (= :all column-name-seq-or-map)
            columns
            map-selector?
            (->> column-name-seq-or-map
                 (map (fn [[old-name new-name]]
                        (if-let [col-idx (get colmap old-name)]
                          (let [col (.get columns (unchecked-int col-idx))]
                            (ds-col-proto/set-name col new-name))
                          (throw (Exception.
                                  (format "Failed to find column %s" old-name)))))))
            :else
            (->> column-name-seq-or-map
                 (map (fn [colname]
                        (if-let [col-idx (get colmap colname)]
                          (.get columns (unchecked-int col-idx))
                          (throw (Exception.
                                  (format "Failed to find column %s" colname))))))))]
      (->> columns
           ;;select may be slow if we have to recalculate missing values.
           (map (fn [col]
                  (if indexes
                    (ds-col-proto/select col indexes)
                    col)))
           (new-dataset (ds-proto/dataset-name dataset) metadata))))


  (supported-column-stats [_dataset]
    (ds-col-proto/supported-stats (first columns)))


  dtype-proto/PShape
  (shape [_m]
    [(count columns)
     (if-let [first-col (first columns)]
       (dtype/ecount first-col)
       0)])

  dtype-proto/PCopyRawData
  (copy-raw->item! [_raw-data ary-target target-offset options]
    (dtype-proto/copy-raw->item! columns ary-target target-offset options))
  dtype-proto/PClone
  (clone [item]
    (new-dataset (ds-proto/dataset-name item)
                 metadata
                 (mapv dtype/clone columns)))

  Counted
  (count [_this] (count columns))

  IFn
  ;;NON-OBVIOUS SEMANTICS
  ;;Legacy implementation of invoke differs from clojure idioms for maps,
  ;;and instead of performing a lookup, we have effectively an assoc.
  ;;Is this necessary, or can we better conform to clojure idioms?
  ;; (invoke [item col-name new-col]
  ;;   (ds-proto/add-column item (ds-col-proto/set-name new-col col-name)))

  (invoke [this k]
    (.valAt this k))
  (invoke [this k not-found]
    (.valAt this k not-found))
  (applyTo [this arg-seq]
    (case (count arg-seq)
      1 (.invoke this (first arg-seq))
      2 (.invoke this (first arg-seq) (second arg-seq))))

  IObj
  (meta [_this] metadata)
  (withMeta [_this metadata] (Dataset. columns colmap metadata _hash _hasheq))

  Iterable
  (iterator [_item]
    (.iterator (map-entries columns)))

  Object
  (toString [item]
    (ds-print/dataset->str item)))



(defn new-dataset
  "Create a new dataset from a sequence of columns.  Data will be converted
  into columns using ds-col-proto/ensure-column-seq.  If the column seq is simply a
  collection of vectors, for instance, columns will be named ordinally.
  options map -
    :dataset-name - Name of the dataset.  Defaults to \"_unnamed\".
    :key-fn - Key function used on all column names before insertion into dataset.

  The return value fulfills the dataset protocols."
  ([options ds-metadata column-seq]
   (let [;;Options was dataset-name so have to keep that pathway going.
         dataset-name (or (if (map? options)
                            (:dataset-name options)
                            options)
                          "_unnamed")]
     (if-not (seq column-seq)
       (Dataset. [] {}
                 (assoc (col-impl/->persistent-map ds-metadata)
                        :name
                        dataset-name) -1 -1)
       (let [column-seq (->> (col-impl/ensure-column-seq column-seq)
                             (map-indexed (fn [idx column]
                                            (let [cname (ds-col-proto/column-name
                                                         column)]
                                              (if (or (nil? cname)
                                                      (and (string? cname)
                                                           (empty? cname)))
                                                (ds-col-proto/set-name column idx)
                                                column)))))
             sizes (->> (map dtype/ecount column-seq)
                        distinct
                        vec)
             column-seq (if (== (count sizes) 1)
                          column-seq
                          (let [max-size (long (apply max 0 sizes))]
                            (->> column-seq
                                 (map #(col-impl/extend-column-with-empty
                                        % (- max-size (count %)))))))
             column-seq (if (and (map? options)
                                 (:key-fn options))
                          (let [key-fn (:key-fn options)]
                            (->> column-seq
                                 (map #(ds-col-proto/set-name
                                        %
                                        (key-fn (ds-col-proto/column-name %))))))
                          column-seq)]
         (Dataset. (vec column-seq)
                   (->> column-seq
                        (map-indexed
                         (fn [idx col]
                           [(ds-col-proto/column-name col) idx]))
                        (into {}))
                   (assoc (col-impl/->persistent-map ds-metadata)
                          :name
                          dataset-name)
                   -1 -1)))))
  ([options column-seq]
   (new-dataset options {} column-seq))
  ([column-seq]
   (new-dataset {} {} column-seq)))


(dtype-pp/implement-tostring-print Dataset)


;;pprint and graal native do not play nice with each other.
(graal-native/when-not-defined-graal-native
 (require '[clojure.pprint :as pprint])
 (defmethod pprint/simple-dispatch
   tech.v3.dataset.impl.dataset.Dataset [f] (pr f)))


(defn dataset?
  [ds]
  (instance? Dataset ds))


(defn empty-dataset
  []
  (new-dataset
   "_unnamed"
   nil))
