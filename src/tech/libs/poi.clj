(ns tech.libs.poi
  "xls, xlsx formats."
  (:require [tech.io :as io]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.ml.dataset.parse.spreadsheet :as parse-spreadsheet]
            [clojure.set :as set])
  (:import [java.lang AutoCloseable]
           [org.apache.poi.ss.usermodel Workbook Sheet Cell
            CellType Row]
           [tech.libs Spreadsheet$Workbook Spreadsheet$Sheet
            Spreadsheet$Row Spreadsheet$Cell]
           [tech.v2.datatype.typed_buffer TypedBuffer]
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


(def xls-file "test/data/file_example_XLS_1000.xls")
(def xlsx-file "test/data/file_example_XLSX_1000.xlsx")


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
  "Returns a sequence of dataset named after the sheets.  This supports a subset of the arguments
  for tech.ml.dataset.parse/csv->columns.  Specifically:
  header-row? - Defaults to true, indicates the first row is a header.
  parser-fn -
   - keyword - all columns parsed to this datatype
   - ifn? - called with two arguments: (parser-fn column-name-or-idx column-data)
          - Return value must be implement PColumnParser in which case that is used
            or can return nil in which case the default column parser is used.
   - map - the header-name-or-idx is used to lookup value.  If not nil, then
           can be either of the two above.  Else the default column parser is used.
   - tuple - pair of [datatype parse-fn] in which case container of type [datatype] will be created
             and parse-fn will be called for every non-entry empty and is passed a string.  The return value
             is inserted in the container.  For datetime types, the parse-fn can in addition be a string in
             which case (DateTimeFormatter/ofPattern parse-fn) will be called or parse-fn can be a
             DateTimeFormatter.
  parser-scan-len - Length of initial column data used for parser-fn.  Defaults to 100."
  ([input options]
   (let [workbook (input->workbook input options)]
     (try
       (mapv #(parse-spreadsheet/sheet->dataset % options) workbook)
       (finally
         (when-not (identical? input workbook)
           (.close workbook))))))
  ([workbook]
   (workbook->datasets workbook {})))
