(ns tech.ml.dataset.print
  (:require [tech.ml.protocols.dataset :as ds-proto]
            [tech.ml.protocols.column :as ds-col-proto]
            [tech.ml.dataset.format-sequence :as format-sequence]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.pprint :as dtype-pp]
            [tech.v2.datatype.datetime :as dtype-dt]
            [clojure.string :as str]
            [clojure.pprint :as pp])
  (:import [tech.v2.datatype ObjectReader]
           [java.util List HashMap Collections ArrayList]
           [tech.ml.dataset FastStruct]
           [clojure.lang PersistentStructMap$Def
            PersistentVector]
           [org.roaringbitmap RoaringBitmap]))


(set! *warn-on-reflection* true)

;;The default number of rows to print
(def ^:dynamic *default-table-row-print-length* 25)
;;The default line policy - see dataset-data->str
(def ^:dynamic *default-print-line-policy* :repl)
;;The default max width with 'nil' indicating no limit.
(def ^:dynamic *default-print-column-max-width* nil)



(defn- print-stringify
  [item]
  (-> (if (or (vector? item)
              (map? item)
              (set? item))
        (with-out-str
          (pp/pprint item))
        (dtype-pp/format-object item))
      (str/replace "|" "\\|")))


(defn- reader->string-lines
  [reader-data ^RoaringBitmap missing line-policy column-max-width new-number-format?]
  (let [reader-data (if (and new-number-format?
                             (#{:float32 :float64} (dtype/get-datatype reader-data)))
                      (vec (format-sequence/format-sequence reader-data))
                      reader-data)]
    (dtype/object-reader
     (dtype/ecount reader-data)
     #(if (.contains missing (int %))
        nil
        (let [lines (str/split-lines (print-stringify (reader-data %)))
              lines (if (number? column-max-width)
                      (let [width (long column-max-width)]
                        (->> lines (map (fn [^String line]
                                          (if (> (count line) width)
                                            (.substring line 0 width)
                                            line)))))
                      lines)]
          (case line-policy
            :single
            [(first lines)]
            :markdown
            [(str/join "<br>" lines)]
            :repl
            lines))))))


(defn- append-line!
  [^StringBuilder builder line]
  (.append builder line)
  (.append builder "\n"))


(defn- rpad-str
  [col-width line]
  (let [n-data (count line)
        n-pad (- (long col-width) n-data)
        builder (StringBuilder.)]
    (.append builder line)
    (dotimes [idx n-pad]
      (.append builder " "))
    (.toString builder)))


(defn dataset-data->str
  "Convert the dataset values to a string.

  Options may be provided in the dataset metadata or may be provided
  as an options map.  The options map overrides the dataset metadata.

  :print-index-range - The set of indexes to print.  Defaults to:
    (range *default-table-row-print-length*)
  :print-line-policy - defaults to :repl - one of
    - :repl - multiline table - default nice printing for repl
    - :markdown - lines delimited by <br>
    - :single - Only print first line
  :print-column-max-width - set the max width of a column when printing.

  Example for conservative printing:
tech.ml.dataset.github-test> (def ds (with-meta ds
                                       (assoc (meta ds)
                                              :print-column-max-width 25
                                              :print-line-policy :single)))"
  ([dataset]
   (dataset-data->str dataset {}))
  ([dataset options]
   (let [{:keys [print-index-range print-line-policy print-column-max-width]}
         (merge (meta dataset) options)
         index-range (or print-index-range
                         (range
                          (min (second (dtype/shape dataset))
                               *default-table-row-print-length*)))
         line-policy (or print-line-policy *default-print-line-policy*)
         column-width (or print-column-max-width *default-print-column-max-width*)
         print-ds (ds-proto/select dataset :all index-range)
         column-names (map #(.toString ^Object %)
                           (ds-proto/column-names print-ds))
         string-columns (map #(-> (dtype/->reader %)
                                  (dtype-pp/reader-converter)
                                  (reader->string-lines (ds-col-proto/missing %)
                                                        line-policy
                                                        column-width
                                                        true)
                                  ;;Do the conversion to string once.
                                  (dtype/clone)
                                  (dtype/->reader))
                             (vals print-ds))
         n-rows (long (second (dtype/shape print-ds)))
         row-heights (ArrayList.)
         _ (.addAll row-heights (repeat n-rows 1))
         column-widths
         (->> string-columns
              (map (fn [colname coldata]
                     (->> coldata
                          (map-indexed
                           (fn [row-idx lines]
                             ;;Side effecting record row height.
                             (.set row-heights (int row-idx)
                                   (max (int (.get row-heights row-idx))
                                        (count lines)))
                             (apply max 0 (map count lines))))
                          (apply max (count colname))))
                   column-names))
         spacers (map #(apply str (repeat % "-")) column-widths)
         fmts (map #(str "%" % "s") column-widths)
         fmt-row (fn [leader divider trailer row]
                   (str leader
                        (apply str
                               (interpose
                                divider
                                (map #(format %1 %2) fmts row)))
                        trailer))
         builder (StringBuilder.)]
     (append-line! builder (fmt-row "| " " | " " |" column-names))
     (append-line!
      builder
      (apply str
             (concat (mapcat (fn [spacer dtype]
                               (let [numeric? (and
                                               (casting/numeric-type? dtype)
                                               (not (dtype-dt/datetime-datatype?
                                                     dtype)))]
                                 (concat ["|-"]
                                         spacer
                                         (if numeric?
                                           ":"
                                           "-"))))
                             spacers (map dtype/get-datatype
                                          print-ds))
                     ["|"])))
     (dotimes [idx n-rows]
       (let [row-height (long (.get row-heights idx))]
         (dotimes [inner-idx row-height]
           (let [row-data
                 (->> string-columns
                      (map (fn [c-width column]
                             (let [lines (column idx)]
                               (if (< inner-idx (count lines))
                                 (if (== 1 (count lines))
                                   (.get ^List lines inner-idx)
                                   (->> (.get ^List lines inner-idx)
                                        (rpad-str c-width)))
                                 "")))
                           column-widths))]
             (append-line! builder (fmt-row "| " " | " " |" row-data))))))
     (.toString builder))))


(defn dataset->str
  "Convert a dataset to a string.  Prints a single line header and then calls
  dataset-data->str.

  For options documentation see dataset-data->str."
  ([ds options]
   (if (= [0 0] (dtype/shape ds))
     (format "%s %s"
             (ds-proto/dataset-name ds)
             ;;make row major shape to avoid confusion
             (vec (reverse (dtype/shape ds))))
     (format "%s %s:\n\n%s"
             (ds-proto/dataset-name ds)
             ;;make row major shape to avoid confusion
             (vec (reverse (dtype/shape ds)))
             (dataset-data->str ds options))))
  ([ds]
   (dataset->str ds {})))
