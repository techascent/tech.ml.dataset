(ns tech.libs.fastexcel
  (:require [tech.io :as io]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype :as dtype]
            [tech.ml.dataset.parse.spreadsheet :as parse-spreadsheet])
  (:import [org.dhatim.fastexcel.reader ReadableWorkbook
            Sheet Row Cell CellType]
           [tech.libs Spreadsheet$Workbook Spreadsheet$Sheet
            Spreadsheet$Row Spreadsheet$Cell]))


(set! *warn-on-reflection* true)


(def xlsx-file "test/data/file_example_XLSX_1000.xlsx")


(defn- cell-type->keyword
  [^CellType cell-type]
  (condp = cell-type
    CellType/EMPTY :none
    CellType/NUMBER :float64
    CellType/BOOLEAN :boolean
    CellType/STRING :string))


(defn- formula-type?
  [cell-type]
  (or (= cell-type CellType/FORMULA)
      (= cell-type CellType/ERROR)))


(defn- wrap-cell
  [^Cell cell]
  (reify
    dtype-proto/PDatatype
    (get-datatype [this]
      (let [cell-type (.getType cell)]
        (if (formula-type? cell-type )
          :float64
          (cell-type->keyword cell-type))))
    Spreadsheet$Cell
    (getColumnNum [this] (.getColumnIndex cell))
    (missing [this] (= :none (dtype/get-datatype this)))
    (value [this]
      (if (formula-type? (.getType cell))
        (Double/parseDouble (.getRawValue cell))
        (case (dtype-proto/get-datatype this)
          :none nil
          :string (.getRawValue cell)
          :boolean (.asBoolean cell)
          (double (.asNumber cell)))))
    (doubleValue [this]
      (if (formula-type? (.getType cell))
        (Double/parseDouble (.getRawValue cell))
        (double (.asNumber cell))))
    (boolValue [this] (.asBoolean cell))))


(defn- wrap-row
  [^Row row]
  (reify
    Spreadsheet$Row
    (getRowNum [this] (dec (.getRowNum row)))
    (iterator [this]
      (let [iter (-> (.stream row)
                     (.filter (reify
                                java.util.function.Predicate
                                (test [this val] (not= val nil))))
                     (.iterator))]
        (reify java.util.Iterator
          (hasNext [this] (.hasNext iter))
          (next [this] (wrap-cell (.next iter))))))))

(defn- wrap-sheet
  [^Sheet sheet]
  (reify
    Spreadsheet$Sheet
    (name [this] (.getName sheet))
    (iterator [this]
      (let [iter (.iterator (.openStream sheet))]
        (reify java.util.Iterator
          (hasNext [this] (.hasNext iter))
          (next [this] (wrap-row (.next iter))))))))


(defn input->workbook
  (^Spreadsheet$Workbook [input]
   (input->workbook input {}))
  (^Spreadsheet$Workbook [input options]
   (let [workbook (ReadableWorkbook. (io/input-stream input))]
     (reify
       Spreadsheet$Workbook
       (close [this] (.close workbook))
       (iterator [this]
         (let [sheet-iter (.iterator (.getSheets workbook))]
           (reify java.util.Iterator
             (hasNext [this] (.hasNext sheet-iter))
             (next [this] (wrap-sheet (.next sheet-iter))))))))))


(defn workbook->datasets
  "Returns a sequence of dataset named after the sheets.
   This supports a subset of the arguments for
  tech.ml.dataset.parse/csv->columns.  Specifically:

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
