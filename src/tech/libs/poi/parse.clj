(ns tech.libs.poi.parse
  "xls, xlsx formats."
  (:require [tech.io :as io]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.readers.const :as const-rdr]
            [tech.ml.dataset.impl.column
             :refer [make-container]
             :as col-impl]
            [tech.ml.dataset.parse :as ds-parse]
            [tech.ml.dataset.impl.dataset :as ds-impl]
            [tech.v2.datatype.bitmap :as bitmap]
            [tech.parallel.for :as parallel-for]
            [clojure.set :as set])
  (:import [java.lang AutoCloseable]
           [org.apache.poi.ss.usermodel Workbook Sheet Cell
            CellType Row]
           [tech.libs Spreadsheet$Workbook Spreadsheet$Sheet
            Spreadsheet$Row Spreadsheet$Cell]
           [org.apache.poi.xssf.usermodel XSSFWorkbook]
           [org.apache.poi.hssf.usermodel HSSFWorkbook]
           [org.roaringbitmap RoaringBitmap]
           [java.util ArrayList List HashMap]
           [java.util.function Function]
           [it.unimi.dsi.fastutil.booleans BooleanArrayList]
           [it.unimi.dsi.fastutil.shorts ShortArrayList]
           [it.unimi.dsi.fastutil.ints IntArrayList IntList IntIterator]
           [it.unimi.dsi.fastutil.longs LongArrayList]
           [it.unimi.dsi.fastutil.floats FloatArrayList]
           [it.unimi.dsi.fastutil.doubles DoubleArrayList]))


(set! *warn-on-reflection* true)

(def xls-file "test/data/file_example_XLS_1000.xls")
(def xlsx-file "test/data/file_example_XLSX_1000.xlsx")


(defprotocol PCellColumnParser
  (add-cell-value! [this cell row-idx])
  (column-data [this]))


(defn unify-container-type-cell-type
  [container* cell-dtype]
  (if (nil? @container*)
    (do
      (reset! container* (col-impl/make-container cell-dtype))
      @container*)
    (let [container-dtype (dtype/get-datatype @container*)]
      (if (= container-dtype cell-dtype)
        @container*
        (let [obj-container (ArrayList.)]
          (.addAll obj-container ^List @container*)
          (reset! container* obj-container)
          obj-container)))))


(defn- add-missing-by-row-idx!
  [container ^RoaringBitmap missing ^long row-idx]
  (let [n-elems (dtype/ecount container)]
    (when (< n-elems)
      (let [n-missing (- row-idx n-elems)
            missing-value (get @col-impl/dtype->missing-val-map
                               (dtype/get-datatype container))
            ^List container container]
        (dotimes [idx n-missing]
          (.add container missing-value)
          (.add missing (+ n-elems idx)))))))


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
              (.add ^List container missing-value))
            (let [container (unify-container-type-cell-type
                             container*
                             (dtype/get-datatype cell))]
              (add-missing-by-row-idx! container missing row-idx)
              (case (dtype/get-datatype container)
                :boolean (.add ^BooleanArrayList container
                               (.boolValue cell))
                :string (.add ^List container (.value cell))
                :float64 (.add ^DoubleArrayList container
                               (.doubleValue cell))
                :object (.add ^List container (.value cell)))))))
        (column-data [this]
                     {:data @container*
                      :missing missing}))))


(defn ensure-n-rows
  [coldata ^long n-rows]
  (when coldata
    (let [{:keys [data missing]} coldata]
      (add-missing-by-row-idx! data missing n-rows)
      coldata)))


(defn sheet->dataset
  ([^Spreadsheet$Sheet worksheet]
   (sheet->dataset worksheet {}))
  ([^Spreadsheet$Sheet worksheet {:keys [header-row?]
                                  :or {header-row? true}
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
         last-row-num (atom nil)
         columns (HashMap.)
         col-parser-gen (reify
                          Function
                          (apply [this k]
                            (default-column-parser)))]
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
     (let [column-set (bitmap/->bitmap (concat (keys columns)
                                               (keys header-row)))
           n-columns (inc (.last column-set))
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
               (let [colname (get header-row col-idx col-idx)
                     coldata (get column-data (long col-idx))
                     coldata (if coldata
                               (ensure-n-rows (get column-data col-idx) n-rows)
                               {:data (const-rdr/make-const-reader missing-value :float64 n-rows)
                                :missing (bitmap/->bitmap (range n-rows))})]
                 ;;We have heterogeneous data so if it isn't a specific datatype don't scan
                 ;;the data to make something else.
                 (assoc coldata
                        :name colname
                        :force-datatype? true))))
            (ds-impl/new-dataset (.name worksheet)))))))


(defn- cell-type->keyword
  [^CellType cell-type]
  (condp = cell-type
    CellType/BLANK :none
    CellType/_NONE :none
    CellType/NUMERIC :float64
    CellType/BOOLEAN :boolean
    CellType/STRING :string))


(defn- wrap-cell
  [^Cell cell]
  (reify
    dtype-proto/PDatatype
    (get-datatype [this]
      (let [cell-type (.getCellType cell)]
        (if (or (= cell-type CellType/FORMULA)
                (= cell-type CellType/ERROR))
          (cell-type->keyword (.getCachedFormulaResultType cell))
          (cell-type->keyword (.getCellType cell)))))
    Spreadsheet$Cell
    (getColumnNum [this] (.. cell getAddress getColumn))
    (missing [this] (= :none (dtype/get-datatype this)))
    (value [this]
      (case (dtype-proto/get-datatype this)
        :none nil
        :string (.getStringCellValue cell)
        :boolean (.getBooleanCellValue cell)
        (.getNumericCellValue cell)))
    (doubleValue [this] (.getNumericCellValue cell))
    (boolValue [this] (.getBooleanCellValue cell))))


(defn- wrap-row
  ^Spreadsheet$Row [^Row row]
  (reify
    Spreadsheet$Row
    (getRowNum [this] (.getRowNum row))
    (iterator [this]
      (let [iter (.iterator row)]
        (reify java.util.Iterator
          (hasNext [this] (.hasNext iter))
          (next [this] (wrap-cell (.next iter))))))))


(defn- wrap-sheet
  ^Spreadsheet$Sheet [^Sheet sheet]
  (reify
    Spreadsheet$Sheet
    (name [this] (.getSheetName sheet))
    (iterator [this]
      (let [iter (.iterator sheet)]
        (reify java.util.Iterator
          (hasNext [this] (.hasNext iter))
          (next [this] (wrap-row (.next iter))))))))


(defn- fname->file-type
  [^String fname]
  (if (.endsWith fname "xls")
    :xls
    :xlsx))



(defn input->workbook
  (^Spreadsheet$Workbook [input options]
   (if (instance? Spreadsheet$Workbook input)
     input
     (let [file-type (or (:poi-file-type options)
                         (when (string? input)
                           (fname->file-type input))
                         :xlsx)
           ^Workbook workbook
           (case file-type
             :xlsx (XSSFWorkbook. (io/input-stream input))
             :xls (HSSFWorkbook. (io/input-stream input)))]
       (reify
         Spreadsheet$Workbook
         (iterator [this]
           (let [iter (.iterator workbook)]
             (reify java.util.Iterator
               (hasNext [this] (.hasNext iter))
               (next [this]
                 (wrap-sheet (.next iter))))))
         (close [this] (.close workbook))))))
  (^Spreadsheet$Workbook [input]
   (input->workbook input {})))


(defn workbook->datasets
  "Returns a sequence of dataset named after the sheets."
  ([input options]
   (let [workbook (input->workbook input options)]
     (try
       (mapv #(sheet->dataset % options) workbook)
       (finally
         (when-not (identical? input workbook)
           (.close workbook))))))
  ([workbook]
   (workbook->datasets workbook {})))
