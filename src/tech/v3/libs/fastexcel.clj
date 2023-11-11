(ns tech.v3.libs.fastexcel
  "Parse a dataset in xlsx format.  This namespace auto-registers a handler for
  the 'xlsx' file type so that when using ->dataset, `xlsx` will automatically map to
  `(first (workbook->datasets))`.

  Note that this namespace does **not** auto-register a handler for the `xls` file type.
  `xls` is handled by the poi namespace.


  If you have an xlsx file that contains multiple sheets and you want a dataset
  out of each sheet you have to use `workbook->datasets` as opposed to the higher level
  `->dataset` operator.

  This dependency has a known conflict with cognitec's aws library - see [issue 283](https://github.com/techascent/tech.ml.dataset/issues/283).

  Required Dependencies:

```clojure
  [org.dhatim/fastexcel-reader \"0.12.8\" :exclusions [org.apache.poi/poi-ooxml]]
```"
  (:require [tech.v3.io :as io]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.dataset.io.spreadsheet :as parse-spreadsheet]
            [tech.v3.dataset.io :as ds-io])
  (:import [org.dhatim.fastexcel.reader ReadableWorkbook
            Sheet Row Cell CellType]
           [tech.v3.dataset Spreadsheet$Workbook Spreadsheet$Sheet
            Spreadsheet$Row Spreadsheet$Cell]))


(set! *warn-on-reflection* true)


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


(defn- try-parse-double
  [^Cell cell]
  (try
    (Double/parseDouble (.getRawValue cell))
    (catch Exception _e
      (.getRawValue cell))))


(defn- wrap-cell
  [^Cell cell]
  (reify
    dtype-proto/PElemwiseDatatype
    (elemwise-datatype [this]
      (let [cell-type (.getType cell)]
        (if (formula-type? cell-type)
          (if (number? (try-parse-double cell))
            :float64
            :string)
          (cell-type->keyword cell-type))))
    Spreadsheet$Cell
    (getColumnNum [this] (.getColumnIndex cell))
    (missing [this] (= :none (dtype/get-datatype this)))
    (value [this]
      (if (formula-type? (.getType cell))
        (try-parse-double cell)
        (case (dtype-proto/elemwise-datatype this)
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
          ;;In some cases hasNext throws.
          (hasNext [this] (try (.hasNext iter)
                               (catch Exception _e false)))
          (next [this] (wrap-cell (.next iter))))))))

(defn- wrap-sheet
  [^Sheet sheet]
  (reify
    Spreadsheet$Sheet
    (name [this] (.getName sheet))
    (id [this] (.getId sheet))
    (stableId [this] (.getStableId sheet))
    (iterator [this]
      (let [iter (.iterator (.openStream sheet))]
        (reify java.util.Iterator
          (hasNext [this] (.hasNext iter))
          (next [this] (wrap-row (.next iter))))))))


(defn input->workbook
  "Given an input data source, return an implementation of
  `tech.v3.dataset/Spreadsheet$Workbook`.  This interface allows you
  to iterate through sheets without necessarily parsing them.
  Once you have a spreadsheet, use `tech.v3.dataset.io.spreadsheet/sheet->dataset`
  to get a dataset."
  (^Spreadsheet$Workbook [input]
   (input->workbook input {}))
  (^Spreadsheet$Workbook [input _options]
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
  "Given a workbook, an string filename or an input stream return a sequence of
  dataset named after the sheets.

  Options are a subset of the arguments for tech.v3.dataset/->dataset:

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


(defmethod ds-io/data->dataset :xlsx
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
