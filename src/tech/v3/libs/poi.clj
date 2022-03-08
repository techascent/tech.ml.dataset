(ns tech.v3.libs.poi
  "Parse a dataset in xls or xlsx format.  This namespace auto-registers a handler for
  the `xls` file type so that when using ->dataset, `xls` will automatically map to
  `(first (workbook->datasets))`.

  Note that this namespace does **not** auto-register a handler for the `xlsx` file
  type. `xlsx` is handled by the fastexcel namespace.

  If you have an `xlsx` or `xls` file that contains multiple sheets and you want a
  dataset out of each sheet you have to use `workbook->datasets` as opposed to the
  higher level `->dataset` operator.

  For serializing datasets to xlsx or xls formats please see
  [kixi.large](https://github.com/MastodonC/kixi.large).

  Note that poi has many versions and many version conflicts and for instance the
  docjure library relies on a much older version of poi."
  (:require [tech.v3.io :as io]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.datetime :as dtype-dt]
            [tech.v3.dataset.io.spreadsheet :as parse-spreadsheet]
            [tech.v3.dataset.io :as ds-io])
  (:import [org.apache.poi.ss.usermodel Workbook Sheet Cell
            CellType Row]
           [tech.v3.dataset Spreadsheet$Workbook Spreadsheet$Sheet
            Spreadsheet$Row Spreadsheet$Cell]
           [org.apache.poi.xssf.usermodel XSSFWorkbook]
           [org.apache.poi.hssf.usermodel HSSFWorkbook]
           [org.apache.poi.ss.usermodel DateUtil]))


(set! *warn-on-reflection* true)


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
          (let [dtype-kwd (cell-type->keyword (.getCellType cell))]
            (if (and (identical? :float64 dtype-kwd)
                     (DateUtil/isCellDateFormatted cell))
              :local-date
              dtype-kwd)))))
    Spreadsheet$Cell
    (getColumnNum [this] (.. cell getAddress getColumn))
    (missing [this] (identical? :none (dtype/get-datatype this)))
    (value [this]
      (case (dtype-proto/elemwise-datatype this)
        :none nil
        :string (.getStringCellValue cell)
        :boolean (.getBooleanCellValue cell)
        :local-date (-> (.getLocalDateTimeCellValue cell)
                        (dtype-dt/local-date-time->local-date))
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
  "Given a workbook, an string filename or an input stream return a sequence of
  dataset named after the sheets.

  Options are a subset of the arguments for tech.v3.dataset/->dataset:

  * `:file-type` - either `:xls` or `:xlsx` - inferred from the filename when input is
     a string.  If input is a stream defaults to `:xlsx`.
  * `:header-row?`
  * `:num-rows`
  * `:n-initial-skip-rows`
  * `:parser-fn`"
  ([input options]
   (let [workbook (input->workbook input options)]
     (try
       (mapv #(parse-spreadsheet/sheet->dataset % options) workbook)
       (finally
         (when-not (identical? input workbook)
           (.close workbook))))))
  ([input]
   (workbook->datasets input {})))


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
