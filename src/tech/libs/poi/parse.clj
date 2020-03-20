(ns tech.libs.poi
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
            [tech.ml.dataset.impl.dataset :as ds-impl]
            [tech.v2.datatype.bitmap :as bitmap]
            [tech.parallel.for :as parallel-for]
            [clojure.set :as set])
  (:import [java.lang AutoCloseable]
           [org.apache.poi.ss.usermodel Workbook Sheet Cell
            CellType Row]
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


(defn get-cell-value
  [^Cell cell]
  (condp = (.getCellType cell)
    CellType/BLANK nil
    CellType/BOOLEAN (.getBooleanCellValue cell)
    CellType/STRING (.getStringCellValue cell)
    CellType/FORMULA (.getNumericCellValue cell)
    (.getNumericCellValue cell)))


(defn cell-missing-type?
  [cell-type]
  (or (= cell-type CellType/BLANK)
      (= cell-type CellType/_NONE)))


(defn- cell-type->datatype
  [cell-type]
    (condp = cell-type
      CellType/NUMERIC :float64
      CellType/BOOLEAN :boolean
      CellType/STRING :string
      CellType/FORMULA :float64
      CellType/ERROR :string))


(extend-type Cell
  dtype-proto/PDatatype
  (get-datatype [this ^Cell cell]
    (let [cell-type (.getCellType cell)]
      (if (= cell-type CellType/FORMULA)
        (cell-type->datatype (.getCachedFormulaResultType cell))
        (cell-type->datatype cell-type)))))


(defn unify-container-type-cell-type
  [container* cell-type]
  (let [cell-dtype (cell-type->datatype cell-type)]
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
            obj-container))))))


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
        (let [^Cell cell cell
              row-idx (long row-idx)]
          (if (cell-missing-type? cell)
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
                             (.getCellType cell))]
              (add-missing-by-row-idx! container missing row-idx)
              (case (dtype/get-datatype container)
                :boolean (.add ^BooleanArrayList container
                               (.getBooleanCellValue cell))
                :string (.add ^List container (.getStringCellValue cell))
                :float64 (.add ^DoubleArrayList container
                               (.getNumericCellValue cell))
                :object (.add ^List container (get-cell-value cell)))))))
        (column-data [this]
                     {:data @container*
                      :missing missing}))))


(defn ensure-n-rows
  [coldata ^long n-rows]
  (when coldata
    (let [{:keys [data missing]} coldata]
      (add-missing-by-row-idx! data missing (dec n-rows))
      coldata)))


(defn sheet->dataset
  ([^Sheet worksheet]
   (sheet->dataset worksheet {}))
  ([^Sheet worksheet {:keys [header-row?]
                      :or {header-row? true}
                      :as options}]
   (let [rows (iterator-seq (.iterator worksheet))
         [header-row rows]
         (if header-row?
           ;;Always have to keep in mind that columns are sparse.
           [(->> (first rows)
                 (map (fn [^Cell cell]
                        (let [column-number
                              (.. cell getAddress getColumn)]
                          [column-number (get-cell-value cell)])))
                 (into {}))
            (rest rows)]
           [nil rows])
         last-row-num (atom nil)
         columns (HashMap.)
         col-parser-gen (reify
                          Function
                          (apply [this k]
                            (default-column-parser)))]
     (doseq [^Row row rows]
       (let [row-num (.getRowNum row)
             row-num (long (if header-row?
                             (dec row-num)
                             row-num))]
         (parallel-for/doiter
          cell row
          (let [^Cell cell cell
                column-number (.. cell getAddress getColumn)
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
                     coldata (or (ensure-n-rows (get column-data col-idx) n-rows)
                                 {:data (const-rdr/make-const-reader missing-value :float64 n-rows)
                                  :missing (bitmap/->bitmap (range n-rows))})]
                 (assoc coldata :name colname))))
            (ds-impl/new-dataset (.getSheetName worksheet)))))))


(defn- fname->file-type
  [^String fname]
  (if (.endsWith fname "xls")
    :xls
    :xlsx))


(defn input->workbook
  (^Workbook [input options]
   (if (instance? Workbook input)
     input
     (let [file-type (or (:poi-file-type options)
                         (when (string? input)
                           (fname->file-type input))
                         :xlsx)]
       (case file-type
         :xlsx (XSSFWorkbook. (io/input-stream input))
         :xls (HSSFWorkbook. (io/input-stream input))))))
  (^Workbook [input]
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
