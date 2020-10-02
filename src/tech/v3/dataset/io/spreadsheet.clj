(ns ^:no-doc tech.v3.dataset.io.spreadsheet
  "Spreadsheets in general are stored in a cell-based format.  This means that any cell
  could have data of any type.  Commonalities around parsing spreadsheet-type systems
  are captured here."
  (:require [tech.v3.dataset.io.context :as parse-context]
            [tech.v3.dataset.io.column-parsers :as column-parsers]
            [tech.v3.datatype :as dtype])
  (:import [tech.v3.dataset Spreadsheet$Sheet Spreadsheet$Row Spreadsheet$Cell]
           [java.util.function Function]
           [java.util HashMap]))


(set! *warn-on-reflection* true)


(defn sheet->dataset
  [^Spreadsheet$Sheet sheet
   {:keys [header-row? n-initial-skip-rows]
    :or {header-row? true}
    :as options}]
  (let [ds-name (or (:dataset-name options)
                    (.name sheet)
                    :_unnamed)
        options (assoc options :dataset-name ds-name)
        rows (iterator-seq (.iterator sheet))
        rows (if n-initial-skip-rows
               (drop-while #(< (.getRowNum ^Spreadsheet$Row %)
                               (long n-initial-skip-rows))
                           rows)
               rows)
        [header-row rows]
        (if header-row?
          ;;Always have to keep in mind that columns are sparse.
          [(->> (first rows)
                (map (fn [^Spreadsheet$Cell cell]
                       (let [column-number (.getColumnNum cell)]
                         [column-number (.value cell)])))
                (into {}))
            (rest rows)]
          [{} rows])
        {:keys [parsers col-idx->parser]}
        (parse-context/options->col-idx-parse-context options :object header-row)]
    (doseq [^Spreadsheet$Row row rows]
      (let [row-num (.getRowNum row)
            row-num (if header-row?
                      (dec row-num)
                      row-num)]
        (doseq [^Spreadsheet$Cell cell row]
          (when-not (.missing cell)
            (let [parser (col-idx->parser (.getColumnNum cell))]
              (case (dtype/elemwise-datatype cell)
                :boolean (column-parsers/add-value! parser row-num (.boolValue cell))
                :float64 (column-parsers/add-value! parser row-num (.doubleValue cell))
                (column-parsers/add-value! parser row-num (.value cell))))))))
    (parse-context/parsers->dataset options parsers)))
