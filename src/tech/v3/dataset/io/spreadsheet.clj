(ns ^:no-doc tech.v3.dataset.io.spreadsheet
  "Spreadsheets in general are stored in a cell-based format.  This means that any cell
  could have data of any type.  Commonalities around parsing spreadsheet-type systems
  are captured here."
  (:require [tech.v3.dataset.io.context :as parse-context]
            [tech.v3.dataset.io.column-parsers :as column-parsers]
            [tech.v3.datatype :as dtype]
            [tech.v3.dataset.utils :as utils])
  (:import [tech.v3.dataset Spreadsheet$Sheet Spreadsheet$Row Spreadsheet$Cell]))


(set! *warn-on-reflection* true)

(defn- maybe-ensure-unique-headers
  "Ensures uniqueness of headers in the given collection.
  If 'ensure-unique-headers?' is true, it modifies duplicate headers
  by appending a random string to them."
  [ensure-unique-headers? headers]
  (if ensure-unique-headers?
    (:headers (reduce (fn [{:keys [seen?] :as acc} [column-number header]]
                        (let [header-value (if (seen? header)
                                             (str (utils/column-safe-name header) "_" (utils/rand-str 5))
                                             header)]
                          (-> acc
                              (update :seen? conj header)
                              (update :headers conj [column-number header-value]))))
                      {:seen? #{}}
                      headers))
    headers))

(defn sheet->dataset
  [^Spreadsheet$Sheet sheet
   {:keys [ensure-unique-headers? header-row? n-initial-skip-rows]
    :or {header-row? true}
    :as options}]
  (let [ds-name (or (:dataset-name options)
                    (.name sheet)
                    :_unnamed)
        options (assoc options :dataset-name ds-name)
        rows (iterator-seq (.iterator sheet))
        n-initial-skip-rows (long (or n-initial-skip-rows 0))
        rows (if-not (== 0 n-initial-skip-rows)
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
                (maybe-ensure-unique-headers ensure-unique-headers?)
                (into {}))
            (rest rows)]
          [{} rows])
        {:keys [parsers col-idx->parser]}
        (parse-context/options->col-idx-parse-context options :object header-row)
        row-dec (+ n-initial-skip-rows (if header-row? 1 0))]
    (doseq [^Spreadsheet$Row row rows]
      (let [row-num (- (.getRowNum row) row-dec)]
        (doseq [^Spreadsheet$Cell cell row]
          (let [parser (col-idx->parser (.getColumnNum cell))]
            (when-not (.missing cell)
              (case (dtype/elemwise-datatype cell)
                :boolean (column-parsers/add-value! parser row-num (.boolValue cell))
                :float64 (column-parsers/add-value! parser row-num (.doubleValue cell))
                (column-parsers/add-value! parser row-num (.value cell))))))))
    ;;Fill out columns that only have missing values
    (when-not (== 0 (count parsers))
      (let [max-col-idx (->> parsers
                             (map-indexed (fn [idx parser]
                                            (when parser idx)))
                             (remove nil?)
                             (last))]
        (dotimes [idx max-col-idx]
          (col-idx->parser idx))))
    (parse-context/parsers->dataset options parsers)))
