(ns ^:no-doc tech.v3.dataset.impl.dataset
  (:require [tech.v3.dataset.protocols :as ds-proto]
            [tech.v3.dataset.impl.column :as col-impl]
            [tech.v3.dataset.print :as ds-print]
            [tech.v3.datatype.pprint :as dtype-pp]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.argtypes :as argtypes]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.bitmap :as bitmap]
            [tech.v3.datatype.unary-pred :as unary-pred]
            [tech.v3.dataset.impl.column-data-process :as column-data-process]
            [tech.v3.dataset.impl.column-base :as column-base]
            [tech.v3.datatype.graal-native :as graal-native]
            [ham-fisted.api :as hamf]
            [ham-fisted.lazy-noncaching :as lznc])
  (:import [clojure.lang IPersistentMap IObj IFn Counted MapEntry]
           [java.util Map List LinkedHashSet LinkedHashMap]
           [tech.v3.datatype ObjectReader FastStruct Buffer]
           [org.roaringbitmap RoaringBitmap]))


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
      (ds-proto/is-column? new-col-data)
      (vary-meta new-col-data assoc :name col-name)
      ;;maps are scalars in dtype-land but you can pass in column data maps
      ;;so this next check is a bit hairy
      (or (identical? argtype :scalar)
           ;;This isn't a column data map.  Maps are
          (and (instance? Map new-col-data)
               (not (.containsKey ^Map new-col-data :tech.v3.dataset/data))))
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
                                   (if-not (== 0 n-cols)
                                     (shorten-or-extend n-rows data)
                                     data)))
                         (assoc :tech.v3.dataset/name col-name))]
        (col-impl/new-column map-data)))))


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
  (keySet [_this] (.keySet ^Map colmap))
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
  (assoc [this k v]
    (let [n-cols (ds-proto/column-count this)
          c (coldata->column n-cols
                             (ds-proto/row-count this)
                             k v)
          cidx (get colmap k n-cols)]
      (Dataset. (assoc (or columns []) cidx c) (assoc colmap k cidx) metadata 0 0)))
  (assocEx [this k v]
    (if-not (colmap k)
      (.assoc this k v)
      (throw (ex-info "Key already present" {:k k}))))
  ;;without implements (dissoc pm k) behavior
  (without [this k]
    (if-let [cidx (get colmap k)]
      (Dataset. (hamf/concatv (hamf/subvec columns 0 cidx) (hamf/subvec columns (inc cidx)))
                (dissoc colmap k)
                metadata 0 0)
      this))
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
    (when (== _hash 0)
      (set! _hash (clojure.lang.APersistentMap/mapHash  this)))
    _hash)

  clojure.lang.IHashEq
  ;;intentionally using seq instead of iterator for now.
  (hasheq [this]
    (when (== _hasheq 0)
      (set! _hasheq (hash-unordered-coll (or (.seq this)
                                             []))))
    _hasheq)

  ;;DOUBLE CHECK equals/equiv semantics...
  (equals [this o] (or (identical? this o)
                       (clojure.lang.APersistentMap/mapEquals this o)))

  (equiv [this o] (or (identical? this o)
                      (map-equiv this o)))


  ds-proto/PRowCount
  (row-count [this]
    (if (== 0 (.size columns))
      0
      (dtype/ecount (.get columns 0))))


  ds-proto/PColumnCount
  (column-count [this]
    (.size columns))


  ds-proto/PMissing
  (missing [this]
    (hamf/reduce (fn [acc col]
                   (.or ^RoaringBitmap acc
                        (bitmap/->bitmap (ds-proto/missing col)))
                   acc)
                 (RoaringBitmap.)
                 columns))

  ds-proto/PSelectRows
  (select-rows [dataset rowidxs]
    (let [rowidxs (col-impl/simplify-row-indexes (ds-proto/row-count dataset) rowidxs)]
      (->> columns
           ;;select may be slower if we have to recalculate missing values.
           (lznc/map #(ds-proto/select-rows % rowidxs))
           (new-dataset (ds-proto/dataset-name dataset) metadata))))


  ds-proto/PSelectColumns
  (select-columns [dataset colnames]
    ;;Conversion to a reader is expensive in some cases so do it here
    ;;to avoid each column doing it.
    (let [map-selector? (instance? Map colnames)]
      (->> (cond
             (identical? :all colnames)
             columns
             map-selector?
             (->> colnames
                  (lznc/map (fn [[old-name new-name]]
                              (if-let [col-idx (get colmap old-name)]
                                (vary-meta (.get columns (unchecked-int col-idx))
                                           assoc :name new-name)
                                (throw (Exception.
                                        (format "Failed to find column %s" old-name)))))))
             :else
             (->> colnames
                  (lznc/map (fn [colname]
                              (if-let [col-idx (get colmap colname)]
                                (.get columns (unchecked-int col-idx))
                                (throw (Exception.
                                        (format "Failed to find column %s" colname))))))))
           (new-dataset (ds-proto/dataset-name dataset) metadata))))

  ds-proto/PDataset
  (is-dataset? [item] true)
  (column [ds cname]
    (if-let [retval (.get ds cname)]
      retval
      (throw (RuntimeException. (str "Column not found: " cname)))))

  (rowvecs [ds options]
    (let [readers (hamf/mapv dtype/->reader columns)
          n-cols (count readers)
          n-rows (ds-proto/row-count ds)
          copying? (get options :copying?)]
      (if (and copying? (< n-cols 7))
        (case n-cols
          0 (reify ObjectReader
              (lsize [rdr] n-rows)
              (readObject [rdr row-idx] []))
          1 (let [c0 (readers 0)]
              (reify ObjectReader
                (lsize [rdr] n-rows)
                (readObject [rdr row-idx] [(c0 row-idx)])))
          2 (let [c0 (readers 0)
                  c1 (readers 1)]
              (reify ObjectReader
                (lsize [rdr] n-rows)
                (readObject [rdr row-idx] [(c0 row-idx) (c1 row-idx)])))
          3 (let [c0 (readers 0)
                  c1 (readers 1)
                  c2 (readers 2)]
              (reify ObjectReader
                (lsize [rdr] n-rows)
                (readObject [rdr row-idx] [(c0 row-idx) (c1 row-idx)
                                           (c2 row-idx)])))
          4 (let [c0 (readers 0)
                  c1 (readers 1)
                  c2 (readers 2)
                  c3 (readers 3)]
              (reify ObjectReader
                (lsize [rdr] n-rows)
                (readObject [rdr row-idx] [(c0 row-idx) (c1 row-idx)
                                           (c2 row-idx) (c3 row-idx)])))
          5 (let [c0 (readers 0)
                  c1 (readers 1)
                  c2 (readers 2)
                  c3 (readers 3)
                  c4 (readers 4)]
              (reify ObjectReader
                (lsize [rdr] n-rows)
                (readObject [rdr row-idx] [(c0 row-idx) (c1 row-idx)
                                           (c2 row-idx) (c3 row-idx)
                                           (c4 row-idx)])))
          6 (let [c0 (readers 0)
                  c1 (readers 1)
                  c2 (readers 2)
                  c3 (readers 3)
                  c4 (readers 4)
                  c5 (readers 5)]
              (reify ObjectReader
                (lsize [rdr] n-rows)
                (readObject [rdr row-idx] [(c0 row-idx) (c1 row-idx)
                                           (c2 row-idx) (c3 row-idx)
                                           (c4 row-idx) (c5 row-idx)]))))
        (if copying?
          (reify ObjectReader
            (lsize [rdr] n-rows)
            (readObject [rdr row-idx]
              (hamf/mapv #((readers %) row-idx) (range n-cols))))
          (reify ObjectReader
            (lsize [rdr] n-rows)
            (readObject [rdr row-idx]
              ;;in-place reads
              (reify ObjectReader
                (lsize [this] n-cols)
                (readObject [this col-idx]
                  (.get ^List (readers col-idx) row-idx)))))))))

  (rows [ds options]
    (let [rvecs (.rowvecs ds options)
          colnamemap (LinkedHashMap.)
         _ (doseq [[c-name c-idx] (->> columns
                                       (lznc/map (comp :name meta))
                                       (lznc/map-indexed #(vector %2 (int %1))))]
             (.put colnamemap c-name c-idx))
          n-rows (dtype/ecount rvecs)]
      (reify ObjectReader
        (lsize [rdr] n-rows)
        (readObject [rdr idx]
          (FastStruct. colnamemap (rvecs idx))))))


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
                          (:name ds-metadata)
                          "_unnamed")
         column-seq (hamf/vec column-seq)]
     (if-not (seq column-seq)
       (Dataset. [] {}
                 (assoc (col-impl/->persistent-map ds-metadata)
                        :name
                        dataset-name) 0 0)
       (let [column-seq (hamf/vec column-seq)
             sizes (->> column-seq
                        (lznc/map (fn [data]
                                    (let [data (if (map? data)
                                                 (get data :tech.v3.dataset/data data)
                                                 data)
                                          argtype (argtypes/arg-type data)]
                                      ;;nil return expected
                                      (if (or (identical? argtype :scalar)
                                              (identical? argtype :iterable))
                                        1
                                        (dtype/ecount data)))))
                        (lznc/remove nil?)
                        (hamf/immut-set))
             n-rows (long (if (== 0 (count sizes))
                            0
                            (apply max sizes)))
             n-cols (count column-seq)
             key-fn (or (when (map? options)
                          (get options :key-fn identity))
                        identity)
             column-seq (->> column-seq
                             (lznc/map-indexed
                              (fn [idx column]
                                (let [cname (ds-proto/column-name column)
                                      cname (if (or (nil? cname)
                                                    (and (string? cname)
                                                         (empty? cname)))
                                              (key-fn idx)
                                              (key-fn cname))]
                                  (coldata->column n-cols n-rows cname column))))
                             (hamf/vec))]
         (Dataset. column-seq
                   (->> column-seq
                        (lznc/map-indexed
                         (fn [idx col]
                           [(ds-proto/column-name col) idx]))
                        (into {}))
                   (assoc (col-impl/->persistent-map ds-metadata)
                          :name
                          dataset-name)
                   0 0)))))
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


(def ^:private EMPTY (new-dataset "_unnamed" nil))


(defn empty-dataset [] EMPTY)
