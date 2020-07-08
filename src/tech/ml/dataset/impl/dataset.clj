(ns tech.ml.dataset.impl.dataset
  (:require [tech.ml.protocols.column :as ds-col-proto]
            [tech.ml.protocols.dataset :as ds-proto]
            [tech.ml.dataset.impl.column :as col-impl]
            [tech.ml.dataset.print :as ds-print]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.bitmap :as bitmap])
  (:import [java.io Writer]
           [clojure.lang IPersistentMap IObj IFn Counted Indexed]
           [java.util Map List LinkedHashSet]))


(set! *warn-on-reflection* true)


(declare new-dataset)

;;ported from clojure.lang.APersistentMap
(defn map-equiv [this o]
  (cond (not (instance? java.util.Map o)) false
        (and (instance? clojure.lang.IPersistentMap o)
             (not (instance? clojure.lang.MapEquivalence o))) false
        :else (let [^java.util.Map m o]
                (and (= (.size m) (count this))
                     (every? (fn [k]
                               (.containsKey m k)) (keys this))))))

(deftype Dataset [^List columns
                  colmap
                  ^IPersistentMap metadata
                  ^{:unsynchronized-mutable true :tag 'int}  _hash
                  ^{:unsynchronized-mutable true :tag 'int}  _hasheq]
  java.util.Map
  (size [this]    (.count this))
  (isEmpty [this] (pos? (.count this)))
  (containsValue [this v] (some #(= % v) columns))
  (get [this k] (.valAt this k))
  (put [this k v]  (throw (UnsupportedOperationException.)))
  (remove [this k] (throw (UnsupportedOperationException.)))
  (putAll [this m] (throw (UnsupportedOperationException.)))
  (clear [this]    (throw (UnsupportedOperationException.)))
  (keySet [this] (set (keys colmap)))
  (values [this]    columns)
  (entrySet [this]
    (let [retval (LinkedHashSet.)]
      (doseq [col columns]
        (.add retval (clojure.lang.MapEntry. (:name (meta col)) col)))
      retval))

  clojure.lang.ILookup
  (valAt [this k]
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
  (empty [this] (new-dataset
                 "_unnamed"
                 {:name "_unnamed"}
                 []))
  ;;ported from clojure java impl.
  (cons [this e]
    (cond (instance? java.util.Map$Entry e)
            (.assoc this (key e) (val e))
          (vector? e)
            (let [^clojure.lang.PersistentVector e e
                  _  (when-not (== (.count e) 2)
                       (throw (ex-info "Vector arg to map conj must be a pair")))]
              (.assoc this (.nth e 0) (.nth e 1)))
          :else
            (reduce (fn [^clojure.lang.Associative acc entry]
                      (.assoc acc (key entry) (val entry))) this e)))
  (containsKey [this k]  (and (colmap k) true))

  ;;MAJOR DEVIATION
  ;;This conforms to clojure's idiom and projects the dataset onto a
  ;;seq of [column-name column] entries.  Legacy implementation defaulted
  ;;to using iterable, which was a seq of column.
  (seq [this]
    ;;Do not reorder column data if possible.
    (map #(clojure.lang.MapEntry. (:name (meta %)) %) columns))

  ;;Equality is likely a rat's nest, although we should be able to do it
  ;;if we wanted to!
  (hashCode [this]
    (when (zero? _hash)
      (set! _hash (clojure.lang.APersistentMap/mapHash this)))
    _hash)

  clojure.lang.IHashEq
  ;;intentionally using seq instead of iterator for now.
  (hasheq [this]
    (when (zero? _hasheq)
      (set! _hasheq (hash-unordered-coll (.seq this))))
    _hasheq)

  ;;DOUBLE CHECK equals/equiv semantics...
  (equals [this o] (or (identical? this o)
                       (instance? clojure.lang.IHashEq o) (== (hash this) (hash o))
                       (clojure.lang.APersistentMap/mapEquals this o)))

  (equiv [this o] (or (identical? this o)
                      (and (instance? clojure.lang.IHashEq o) (== (hash this) (hash o)))
                      (map-equiv this o)))

  ds-proto/PColumnarDataset
  (dataset-name [dataset] (:name metadata))
  (set-dataset-name [dataset new-name]
    (Dataset. columns colmap
     (assoc metadata :name new-name) _hash _hasheq))
  (maybe-column [dataset column-name]
    (when-let [idx (get colmap column-name)]
      (.get columns (int idx))))

  (metadata [dataset] metadata)
  (set-metadata [dataset meta-map]
    (Dataset. columns colmap
              (col-impl/->persistent-map meta-map)
              _hash _hasheq))

  (columns [dataset] columns)

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
          new-col-data (col-fn col)]
      (Dataset.
       (assoc columns col-idx
              (if (ds-col-proto/is-column? new-col-data)
                (ds-col-proto/set-name new-col-data col-name)
                (col-impl/new-column (ds-col-proto/column-name col)
                                     (col-impl/ensure-column-reader new-col-data))))
       colmap
       metadata
       0
       0)))

  (add-or-update-column [dataset col-name new-col-data]
    (let [col-data (if (ds-col-proto/is-column? new-col-data)
                     (ds-col-proto/set-name new-col-data col-name)
                     (col-impl/new-column col-name
                                          (col-impl/ensure-column-reader
                                           new-col-data)))]
      (if (contains? colmap col-name)
        (ds-proto/update-column dataset col-name (constantly col-data))
        (ds-proto/add-column dataset col-data))))

  (select [dataset column-name-seq-or-map index-seq]
    ;;Conversion to a reader is expensive in some cases so do it here
    ;;to avoid each column doing it.
    (let [map-selector? (instance? Map column-name-seq-or-map)
          indexes (if (= :all index-seq)
                    nil
                    (if-let [bmp (dtype/as-roaring-bitmap index-seq)]
                      (dtype/->reader
                       (bitmap/bitmap->efficient-random-access-reader
                        bmp))
                      (if (dtype/reader? index-seq)
                        (dtype/->reader index-seq)
                        (dtype/->reader (dtype/make-container
                                         :java-array :int32
                                         index-seq)))))
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

  (select-columns-by-index [dataset num-seq]
    (let [col-indexes (int-array (distinct num-seq))]
      (when-not (== (count col-indexes) (count num-seq))
        (throw (Exception. (format "Duplicate column selection detected: %s"
                                   num-seq))))
      (->> col-indexes
           (map (fn [^long idx]
                  (.get columns idx)))
           (new-dataset (ds-proto/dataset-name dataset) metadata))))


  (supported-column-stats [dataset]
    (ds-col-proto/supported-stats (first columns)))


  (from-prototype [dataset dataset-name column-seq]
    (new-dataset dataset-name column-seq))


  dtype-proto/PShape
  (shape [m]
    [(count columns)
     (if-let [first-col (first columns)]
       (dtype/ecount first-col)
       0)])

  dtype-proto/PCopyRawData
  (copy-raw->item! [raw-data ary-target target-offset options]
    (dtype-proto/copy-raw->item! columns ary-target target-offset options))
  dtype-proto/PClone
  (clone [item]
    (new-dataset (ds-proto/dataset-name item)
                 metadata
                 (mapv dtype/clone columns)))

  Counted
  (count [this] (count columns))

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
  (meta [this] metadata)
  (withMeta [this metadata] (Dataset. columns colmap metadata _hash _hasheq))

  Iterable
  (iterator [item]
    (->> ^java.lang.Iterable
         (ds-proto/columns item)
         (.iterator)))
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
                        dataset-name) 0 0)
       (let [column-seq (->> (col-impl/ensure-column-seq column-seq)
                             (map-indexed (fn [idx column]
                                            (let [cname (ds-col-proto/column-name
                                                         column)]
                                              (if (and (string? cname)
                                                       (empty? cname))
                                                (ds-col-proto/set-name column idx)
                                                column)))))
             sizes (->> (map dtype/ecount column-seq)
                        distinct)
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
                   0 0)))))
  ([options column-seq]
   (new-dataset options {} column-seq))
  ([column-seq]
   (new-dataset {} {} column-seq)))


(defmethod print-method Dataset
  [^Dataset dataset w]
  (.write ^Writer w ^String (.toString dataset)))


(defn item-val->string
  [item-val item-dtype item-name]
  (cond
    (string? item-val) item-val
    (keyword? item-val) item-val
    (symbol? item-val) item-val
    :else
    (str item-val)))
