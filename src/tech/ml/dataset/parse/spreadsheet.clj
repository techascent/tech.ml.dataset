(ns tech.ml.dataset.parse.spreadsheet
  "Spreadsheets in general are stored in a cell-based format.  This means that any cell
  could have data of any type.  Commonalities around parsing spreadsheet-type systems
  are captured here."
  (:require [tech.v2.datatype :as dtype]
            [tech.v2.datatype.bitmap :as bitmap]
            [tech.v2.datatype.datetime :as dtype-dt]
            [tech.v2.datatype.readers.const :as const-rdr]
            [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.double-ops :as double-ops]
            [tech.v2.datatype.builtin-op-providers :as builtin-op-providers]
            [tech.ml.dataset.impl.column
             :refer [make-container]
             :as col-impl]
            [tech.ml.dataset.impl.dataset :as ds-impl]
            [tech.ml.dataset.parse.datetime :as parse-dt]
            [tech.ml.dataset.parse :as parse]
            [tech.ml.dataset.string-table :as str-table]
            [tech.parallel.for :as parallel-for])
  (:import [java.util List ArrayList HashMap]
           [java.util.function Function]
           [org.roaringbitmap RoaringBitmap]
           [tech.libs Spreadsheet$Workbook Spreadsheet$Sheet
            Spreadsheet$Row Spreadsheet$Cell]
           [java.time.format DateTimeFormatter]
           [tech.v2.datatype.typed_buffer TypedBuffer]
           [it.unimi.dsi.fastutil.booleans BooleanArrayList]
           [it.unimi.dsi.fastutil.longs LongList]
           [it.unimi.dsi.fastutil.ints IntList]
           [it.unimi.dsi.fastutil.doubles DoubleArrayList]))



(defprotocol PCellColumnParser
  (add-cell-value! [this cell row-idx])
  (column-data [this]))


(def packable-datatype-set (set dtype-dt/packable-datatypes))
(def packed-datatype-set (->> packable-datatype-set
                              (map dtype-dt/unpacked-type->packed-type)
                              (set)))


(defn- unify-container-type-cell-type
  [container* container-dtype cell-dtype]
  (let [packed-cell-dtype (if (packable-datatype-set cell-dtype)
                            (dtype-dt/unpacked-type->packed-type cell-dtype)
                            cell-dtype)]
    (if (nil? container-dtype)
      (do
        (reset! container* (col-impl/make-container packed-cell-dtype
                                                    (if @container*
                                                      (dtype/ecount @container*)
                                                      0)))
        @container*)
      (let [container-dtype (dtype/get-datatype @container*)]
        (cond
          (= container-dtype packed-cell-dtype)
          @container*
          (and (casting/numeric-type? container-dtype)
               (casting/numeric-type? cell-dtype)
               (not (dtype-dt/datetime-datatype? cell-dtype))
               (not (dtype-dt/datetime-datatype? container-dtype)))
          (let [new-datatype (builtin-op-providers/widest-datatype
                              container-dtype
                              cell-dtype)
                new-container (if (= new-datatype container-dtype)
                                @container*
                                (dtype/make-container :list new-datatype @container*))]
            (reset! container* new-container)
            new-container)
          :else
          (let [obj-container (ArrayList.)]
            (.addAll obj-container ^List (if (packed-datatype-set container-dtype)
                                           (dtype-dt/unpack @container*)
                                           @container*))
            (reset! container* obj-container)
            obj-container))))))


(defn- add-missing-by-row-idx!
  [container ^RoaringBitmap missing ^long row-idx]
  (let [n-elems (dtype/ecount container)]
    (when (< n-elems row-idx)
      (let [n-missing (- row-idx n-elems)
            missing-value (get @col-impl/dtype->missing-val-map
                               (dtype/get-datatype container))
            ^List container container]
        (dotimes [idx n-missing]
          (if (dtype-dt/datetime-datatype? (dtype/get-datatype container))
            (parse-dt/add-to-container! (dtype/get-datatype container) container
                                        missing-value)
            (.add container missing-value))
          (.add missing (+ n-elems idx)))))))


(defn- add-value-to-container
  [container-dtype container cell-value]
  (if (dtype-dt/datetime-datatype? container-dtype)
    (if (dtype-dt/packable-datatypes (dtype/get-datatype cell-value))
      (parse-dt/add-to-container! container-dtype container
                                  (dtype-dt/pack cell-value))
      (parse-dt/add-to-container! container-dtype container
                                  cell-value))
    (case container-dtype
      :boolean (.add ^BooleanArrayList container
                     (boolean cell-value))
      :string (.add ^List container cell-value)
      :float64 (.add ^DoubleArrayList container
                     (double cell-value))
      (.add ^List container cell-value))))


(defn default-column-parser
  []
  (let [container* (atom nil)
        missing (bitmap/->bitmap)]
    (reify
      PCellColumnParser
      (add-cell-value! [this cell row-idx]
        (let [^Spreadsheet$Cell cell cell
              row-idx (long row-idx)]
          (if (.missing cell)
            (let [container (if (nil? @container*)
                              (do
                                (reset! container* (DoubleArrayList.))
                                @container*)
                              @container*)
                  container-dtype (dtype/get-datatype container)
                  missing-value (get @col-impl/dtype->missing-val-map
                                     container-dtype)]
              (add-missing-by-row-idx! container missing row-idx)
              (.add missing (dtype/ecount container))
              (add-value-to-container container-dtype container missing-value))
            (let [container-dtype (when-not (= (dtype/ecount missing)
                                               (dtype/ecount @container*))
                                    (dtype/get-datatype @container*))
                  original-cell-dtype (dtype/get-datatype cell)
                  [cell-dtype cell-value]
                  (if (= :string original-cell-dtype)
                    (parse-dt/try-parse-datetimes (.value cell))
                    [original-cell-dtype (case original-cell-dtype
                                           :boolean (.boolValue cell)
                                           :float64 (.doubleValue cell)
                                           (.value cell))])
                  container (unify-container-type-cell-type
                             container*
                             container-dtype
                             cell-dtype)
                  container-dtype (dtype/get-datatype container)]
              (add-missing-by-row-idx! container missing row-idx)
              (add-value-to-container container-dtype container cell-value)))))
        (column-data [this]
                     {:data @container*
                      :missing missing}))))


(defn ensure-n-rows
  [coldata ^long n-rows]
  (when coldata
    (let [{:keys [data missing]} coldata]
      (add-missing-by-row-idx! data missing n-rows)
      coldata)))



(defn cell-str-value
  ^String [^Spreadsheet$Cell cell]
  (case (dtype/get-datatype cell)
    :float64 (let [dval (.doubleValue cell)]
               (if (double-ops/is-mathematical-integer? dval)
                 (.toString ^Object (Long. (unchecked-long dval)))
                 (.toString dval)))
    :none ""
    (.toString ^Object (.value cell))))


(defn simple-parser->parser
  ([datatype relaxed?]
   (let [simple-parser
         (if (keyword? datatype)
           (get parse/all-parsers datatype)
           datatype)
         _ (when-not (satisfies? parse/PSimpleColumnParser simple-parser)
             (throw (Exception.
                     "Parse does not satisfy the PSimpleColumnParser protocol")))
         missing-val (col-impl/datatype->missing-value datatype)
         container (parse/make-parser-container simple-parser)
         missing (bitmap/->bitmap)
         unparsed-data (str-table/make-string-table)
         unparsed-indexes (bitmap/->bitmap)
         add-missing-fn #(do
                           (.add missing (unchecked-int (dtype/ecount container)))
                           (.add ^List container missing-val))]
     (reify
       dtype-proto/PDatatype
       (get-datatype [this] datatype)
       PCellColumnParser
       (add-cell-value! [this cell row-idx]
         (let [cell ^Spreadsheet$Cell cell]
           (add-missing-by-row-idx! container missing row-idx)
           (if (.missing cell)
             (add-missing-fn)
             (parse/attempt-simple-parse!
              parse/simple-parse! simple-parser container add-missing-fn
              unparsed-data unparsed-indexes relaxed?
              (cell-str-value cell)))))
       (column-data [this]
         (parse/return-parse-data container missing unparsed-data unparsed-indexes)))))
  ([datatype]
   (simple-parser->parser datatype false)))


(defn general-parser
  [datatype parse-fn]
  (when-not (fn? parse-fn)
    (throw (Exception. (format "parse-fn doesn't appear to be a function: %s"
                               parse-fn))))
  (let [container (make-container datatype)
        missing-val (col-impl/datatype->missing-value datatype)
        missing (bitmap/->bitmap)
        add-fn (if (casting/numeric-type? datatype)
                 #(.add ^List container (casting/cast % datatype))
                 #(.add ^List container %))
        unparsed-data (str-table/make-string-table)
        unparsed-indexes (bitmap/->bitmap)
        add-missing-fn #(do
                          (.add missing (dtype/ecount container))
                          (.add ^List container missing-val))]
    (reify
      dtype-proto/PDatatype
      (get-datatype [this] datatype)
      PCellColumnParser
      (add-cell-value! [this cell row-idx]
        (let [cell ^Spreadsheet$Cell cell]
          (add-missing-by-row-idx! container missing row-idx)
          (if (.missing cell)
            (add-missing-fn)
            (let [str-val (cell-str-value cell)]
              (parse/attempt-general-parse! add-fn parse-fn container add-missing-fn
                                            unparsed-data unparsed-indexes
                                            str-val)))))
      (column-data [this]
        (parse/return-parse-data container missing unparsed-data unparsed-indexes)))))


(defn datetime-formatter-parser
  [datatype format-string-or-formatter]
  (let [parse-fn (parse-dt/datetime-formatter-or-str->parser-fn
                  datatype format-string-or-formatter)]
    (general-parser datatype parse-fn)))


(defn make-parser
  [parser-fn header-row-name scan-rows]
  (parse/make-parser parser-fn header-row-name scan-rows
                     default-column-parser
                     simple-parser->parser
                     datetime-formatter-parser
                     general-parser))


(defn process-spreadsheet-rows
  [rows header-row?
   col-parser-gen
   column-idx->column-name
   options]
  (let [columns (HashMap.)]
    (doseq [^Spreadsheet$Row row rows]
      (let [row-num (.getRowNum row)
            row-num (long (if header-row?
                            (dec row-num)
                            row-num))]
        (parallel-for/doiter
         cell row
         (let [^Spreadsheet$Cell cell cell
               column-number (.getColumnNum cell)
               parser (.computeIfAbsent columns column-number
                                        col-parser-gen)]
           (add-cell-value! parser cell row-num)))))
    ;;This will order the columns
    (let [column-set (bitmap/->bitmap (keys columns))
          n-columns (if-not (.isEmpty column-set)
                      (inc (.last column-set))
                      0)
          column-data (->> columns
                           (map (fn [[k v]]
                                  [(long k) (column-data v)]))
                           (into {}))
          n-rows (->> (vals column-data)
                      (map (comp dtype/ecount :data))
                      (apply max 0)
                      (long))
          missing-value (get @col-impl/dtype->missing-val-map
                             :float64)]
      (->> (range n-columns)
           (mapv
            (fn [col-idx]
              (let [colname (column-idx->column-name col-idx)
                    coldata (get column-data (long col-idx))
                    coldata (if coldata
                              (ensure-n-rows (get column-data col-idx) n-rows)
                              {:data (const-rdr/make-const-reader
                                      missing-value :float64 n-rows)
                               :missing (bitmap/->bitmap (range n-rows))})]
                ;;We have heterogeneous data so if it isn't a specific datatype
                ;;don't scan the data to make something else.
                (assoc coldata
                       :name colname
                       :force-datatype? true))))
           (ds-impl/new-dataset options)))))


(defn scan-initial-rows
  [rows parser-scan-len]
  (->> (take parser-scan-len rows)
       ;;Expand rows into cells
       (mapcat seq)
       ;;Group by column
       (group-by #(.getColumnNum ^Spreadsheet$Cell %))
       ;;Convert lists of cells to lists of strings.  This allows us to share more
       ;;code with the CSV parsing system.
       (map (fn [[k v]]
              [k (map cell-str-value v)]))
       (into {})))


(defn sheet->dataset
  ([^Spreadsheet$Sheet worksheet]
   (sheet->dataset worksheet {}))
  ([^Spreadsheet$Sheet worksheet {:keys [header-row?
                                         parser-fn
                                         parser-scan-len]
                                  :or {header-row? true
                                       parser-scan-len 100}
                                  :as options}]
   (let [rows (iterator-seq (.iterator worksheet))
         [header-row rows]
         (if header-row?
           ;;Always have to keep in mind that columns are sparse.
           [(->> (first rows)
                 (map (fn [^Spreadsheet$Cell cell]
                        (let [column-number (.getColumnNum cell)]
                          [column-number (.value cell)])))
                 (into {}))
            (rest rows)]
           [nil rows])
         scan-rows (when parser-fn
                     (scan-initial-rows rows parser-scan-len))
         col-parser-gen (reify
                          Function
                          (apply [this column-number]
                            (if parser-fn
                              (let [colname (get header-row column-number
                                                 column-number)]
                                (make-parser parser-fn colname
                                             (scan-rows column-number)))
                              (default-column-parser))))]
     (process-spreadsheet-rows rows header-row? col-parser-gen
                               #(get header-row % %)
                               (merge {:dataset-name (.name worksheet)}
                                      options)))))
