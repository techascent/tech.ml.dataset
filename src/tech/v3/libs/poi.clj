(ns tech.v3.libs.poi
  "xls, xlsx formats."
  (:require [tech.io :as io]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.dataset.io.spreadsheet :as parse-spreadsheet]
            [tech.v3.dataset.io :as ds-io])
  (:import [java.lang AutoCloseable]
           [org.apache.poi.ss.usermodel Workbook Sheet Cell
            CellType Row]
           [tech.v3.dataset Spreadsheet$Workbook Spreadsheet$Sheet
            Spreadsheet$Row Spreadsheet$Cell]
           [org.apache.poi.xssf.usermodel XSSFWorkbook]
           [org.apache.poi.hssf.usermodel HSSFWorkbook]
           [org.roaringbitmap RoaringBitmap]
           [java.util ArrayList List HashMap]
           [java.util.function Function]
           [it.unimi.dsi.fastutil.booleans BooleanArrayList]
           [it.unimi.dsi.fastutil.shorts ShortArrayList]
           [it.unimi.dsi.fastutil.ints IntArrayList IntList IntIterator]
           [it.unimi.dsi.fastutil.longs LongArrayList LongList]
           [it.unimi.dsi.fastutil.floats FloatArrayList]
           [it.unimi.dsi.fastutil.doubles DoubleArrayList]))


(set! *warn-on-reflection* true)


(def ^:private xls-file "test/data/file_example_XLS_1000.xls")
(def ^:private xlsx-file "test/data/file_example_XLSX_1000.xlsx")


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
    dtype-proto/PElemwiseDatatype
    (elemwise-datatype [this]
      (let [cell-type (.getCellType cell)]
        (if (or (= cell-type CellType/FORMULA)
                (= cell-type CellType/ERROR))
          (cell-type->keyword (.getCachedFormulaResultType cell))
          (cell-type->keyword (.getCellType cell)))))
    Spreadsheet$Cell
    (getColumnNum [this] (.. cell getAddress getColumn))
    (missing [this] (= :none (dtype/get-datatype this)))
    (value [this]
      (case (dtype-proto/elemwise-datatype this)
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
  "Given an input data source, return an implementation of
  `tech.v3.dataset/Spreadsheet$Workbook`.  This interface allows you
  to iterate through sheets without necessarily parsing them.
  Once you have a spreadsheet, use `tech.v3.dataset.io.spreadsheet/sheet->dataset`
  to get a dataset."
  (^Spreadsheet$Workbook [input options]
   (if (instance? Spreadsheet$Workbook input)
     input
     (let [file-type (or (:file-type options)
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
  "Returns a sequence of dataset named after the sheets.  This supports a subset of
  the arguments for tech.v3.dataset/->dataset.  Specifically:

  * `:header-row?`
  * `:parser-fn`
  * `:parser-scan-len`

  Returns a non-lazy sequence of datasets."
  ([input options]
   (let [workbook (input->workbook input options)]
     (try
       (mapv #(parse-spreadsheet/sheet->dataset % options) workbook)
       (finally
         (when-not (identical? input workbook)
           (.close workbook))))))
  ([workbook]
   (workbook->datasets workbook {})))


(defmethod ds-io/data->dataset :xls
  [data options]
  (let [datasets (workbook->datasets data options)
        n-datasets (count datasets)]
    (errors/when-not-errorf
     (== 1 n-datasets)
     (if (== 0 n-datasets)
       "No (%d) datasets found in file"
       "Multiple (%d) datasets found in file")
     n-datasets)
    (first datasets)))
