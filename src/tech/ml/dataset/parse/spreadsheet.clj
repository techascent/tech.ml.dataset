(ns tech.ml.dataset.parse.spreadsheet
  "Spreadsheets in general are stored in a cell-based format.  This means that any cell
  could have data of any type.  Commonalities around parsing spreadsheet-type systems
  are captured here."
  (:require [tech.v2.datatype :as dtype]
            [tech.v2.datatype.bitmap :as bitmap]
            [tech.v2.datatype.datetime :as dtype-dt]
            [tech.v2.datatype.readers.const :as const-rdr]
            [tech.ml.dataset.impl.column
             :refer [make-container]
             :as col-impl]
            [tech.ml.dataset.impl.dataset :as ds-impl]
            [tech.ml.dataset.parse.datetime :as parse-dt]
            [tech.parallel.for :as parallel-for])
  (:import [java.util List ArrayList HashMap]
           [java.util.function Function]
           [org.roaringbitmap RoaringBitmap]
           [tech.libs Spreadsheet$Workbook Spreadsheet$Sheet
            Spreadsheet$Row Spreadsheet$Cell]
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
        (if (= container-dtype packed-cell-dtype)
          @container*
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
          (.add container missing-value)
          (.add missing (+ n-elems idx)))))))


(defn- try-parse-datetimes
  [str-value]
  (if-let [date-val (try (parse-dt/parse-local-date str-value)
                         (catch Exception e nil))]
    [:local-date date-val]
    (if-let [time-val (try (parse-dt/parse-local-date-time str-value)
                           (catch Exception e nil))]
      [:local-time time-val]
      (if-let [date-time-val (try (parse-dt/parse-local-date-time str-value)
                                  (catch Exception e nil))]
        [:local-date-time date-time-val]
        [:string str-value]))))


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
            (let [container-dtype (when-not (= (dtype/ecount missing)
                                               (dtype/ecount @container*))
                                    (dtype/get-datatype @container*))
                  original-cell-dtype (dtype/get-datatype cell)
                  [cell-dtype cell-value]
                  (if (= :string original-cell-dtype)
                    (try-parse-datetimes (.value cell))
                    [original-cell-dtype (case original-cell-dtype
                                           :boolean (.boolValue cell)
                                           :float64 (.doubleValue cell))])
                  container (unify-container-type-cell-type
                             container*
                             container-dtype
                             cell-dtype)]
              (add-missing-by-row-idx! container missing row-idx)
              (case (dtype/get-datatype container)
                :boolean (.add ^BooleanArrayList container
                               (boolean cell-value))
                :packed-local-date
                (.add ^IntList
                      (.backing_store ^TypedBuffer container)
                      (int (dtype-dt/pack-local-date cell-value)))
                :packed-local-time
                (.add ^IntList
                      (.backing_store ^TypedBuffer container)
                      (int (dtype-dt/pack-local-time cell-value)))
                :packed-local-date-time
                (.add ^LongList
                      (.backing_store ^TypedBuffer container)
                      (long (dtype-dt/pack-local-date-time cell-value)))
                :string (.add ^List container (.value cell))
                :float64 (.add ^DoubleArrayList container
                               (double cell-value))
                :object (.add ^List container cell-value))))))
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
