(ns tech.v3.dataset.print
  (:require [tech.v3.dataset.protocols :as ds-proto]
            [tech.v3.dataset.format-sequence :as format-sequence]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.pprint :as dtype-pp]
            [tech.v3.datatype.datetime :as dtype-dt]
            [tech.v3.datatype.packing :as packing]
            [tech.v3.datatype.graal-native :as graal-native]
            [clojure.string :as str]
            [ham-fisted.lazy-noncaching :as lznc]
            [ham-fisted.api :as hamf]
            [ham-fisted.function :as hamf-fn])
  (:import [tech.v3.datatype ObjectReader]
           [java.util List ArrayList]
           [org.roaringbitmap RoaringBitmap]))


(graal-native/when-not-defined-graal-native
 (require '[clojure.pprint :as pp]))


(set! *warn-on-reflection* true)

;;The default number of rows to print
(def ^:dynamic ^:no-doc *default-table-row-print-length* 20)
;;The default line policy - see dataset-data->str
(def ^:dynamic ^:no-doc *default-print-line-policy* :repl)
;;The default max width with 'nil' indicating no limit.
(def ^:dynamic ^:no-doc *default-print-column-max-width* nil)
;;The default to show/hide column types
(def ^:dynamic ^:no-doc *default-print-column-types?* false)



(defn- print-stringify
  [item]
  (-> (if (or (vector? item)
              (map? item)
              (set? item))
        (with-out-str
          (graal-native/if-defined-graal-native
           (println item)
           (do
             (require '[clojure.pprint :as pp])
             (pp/pprint item))))
        (dtype-pp/format-object item))
      (str/replace "|" "\\|")))


(defn- reader->string-lines
  [reader-data ^RoaringBitmap missing line-policy column-max-width new-number-format?
   maximum-precision]
  (let [reader-data (if (and new-number-format?
                             (#{:float32 :float64} (dtype/elemwise-datatype reader-data)))
                      (vec (format-sequence/format-sequence
                            reader-data
                            (long (or maximum-precision 8))))
                      reader-data)]
    (reify ObjectReader
      (lsize [rdr] (dtype/ecount reader-data))
      (readObject [rdr idx]
        (if (.contains missing (int idx))
           nil
           (let [lines (str/split-lines (print-stringify (reader-data idx)))
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
               lines)))))))


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
    (dotimes [_idx n-pad]
      (.append builder " "))
    (.toString builder)))


(defn dataset-data->str
  "Convert the dataset values to a string.

Options may be provided in the dataset metadata or may be provided
as an options map.  The options map overrides the dataset metadata.


  * `:print-index-range` - The set of indexes to print.  If an integer then
     is interpreted according to `:print-style`.  Defaults to the integer
     `*default-table-row-print-length*`.
  * `:print-style` - Defaults to :first-last.  Options are #{:first-last :first :last}.  In
     the case `:print-index-range` is an integer and the dataset has more than that number of
     rows prints the first N/2 and last N/2 rows or the first N or last N rows.
  * `:print-line-policy` - defaults to `:repl` - one of:
     - `:repl` - multiline table - default nice printing for repl
     - `:markdown` - lines delimited by <br>
     - `:single` - Only print first line
  * `:print-column-max-width` - set the max width of a column when printing.
  * `:print-column-types?` - show/hide column types.
  * `:maximum-precision` - When provided, the maximum double precision as an integer.
  * `:elide-header?` - When true, the header such as `test/data/alldtypes.arrow-feather-compressed [1000 15]:` is hidden.


Examples of print styles:

```clojure
user> (require '[tech.v3.dataset :as ds])
nil
user> (require '[tech.v3.libs.arrow :as arrow])
nil
user> (def ds (ds/->dataset \"test/data/alldtypes.arrow-feather-compressed\" {:file-type :arrow}))
08:26:03.156 [tech.resource.gc ref thread] INFO  tech.v3.resource.gc - Reference thread starting
#'user/ds

user> (vary-meta ds assoc :print-style :last :print-index-range 10)
test/data/alldtypes.arrow-feather-compressed [1000 15]:

| uints | longs | ubytes | strings | doubles | ushorts |  local_times | local_dates | ints |            instants | shorts | bytes | boolean | floats | text |
|------:|------:|-------:|---------|--------:|--------:|--------------|-------------|-----:|--------------------:|-------:|------:|---------|-------:|------|
|   990 |   990 |    222 |     990 |   990.0 |     990 | 13:39:59.923 |  2022-02-19 |  990 | 1645303199916000000 |    990 |   -34 |         |  990.0 |  990 |
|   991 |   991 |    223 |     991 |   991.0 |     991 | 13:39:59.923 |  2022-02-19 |  991 | 1645303199916000000 |    991 |   -33 |         |  991.0 |  991 |
|   992 |   992 |    224 |     992 |   992.0 |     992 | 13:39:59.923 |  2022-02-19 |  992 | 1645303199916000000 |    992 |   -32 |         |  992.0 |  992 |
|   993 |   993 |    225 |     993 |   993.0 |     993 | 13:39:59.923 |  2022-02-19 |  993 | 1645303199916000000 |    993 |   -31 |         |  993.0 |  993 |
|   994 |   994 |    226 |     994 |   994.0 |     994 | 13:39:59.923 |  2022-02-19 |  994 | 1645303199916000000 |    994 |   -30 |         |  994.0 |  994 |
|   995 |   995 |    227 |     995 |   995.0 |     995 | 13:39:59.923 |  2022-02-19 |  995 | 1645303199916000000 |    995 |   -29 |         |  995.0 |  995 |
|   996 |   996 |    228 |     996 |   996.0 |     996 | 13:39:59.923 |  2022-02-19 |  996 | 1645303199916000000 |    996 |   -28 |         |  996.0 |  996 |
|   997 |   997 |    229 |     997 |   997.0 |     997 | 13:39:59.923 |  2022-02-19 |  997 | 1645303199916000000 |    997 |   -27 |         |  997.0 |  997 |
|   998 |   998 |    230 |     998 |   998.0 |     998 | 13:39:59.923 |  2022-02-19 |  998 | 1645303199916000000 |    998 |   -26 |         |  998.0 |  998 |
|   999 |   999 |    231 |     999 |   999.0 |     999 | 13:39:59.923 |  2022-02-19 |  999 | 1645303199916000000 |    999 |   -25 |         |  999.0 |  999 |
user> (vary-meta ds assoc :print-style :first :print-index-range 10)
test/data/alldtypes.arrow-feather-compressed [1000 15]:

| uints | longs | ubytes | strings | doubles | ushorts |  local_times | local_dates | ints |            instants | shorts | bytes | boolean | floats | text |
|------:|------:|-------:|---------|--------:|--------:|--------------|-------------|-----:|--------------------:|-------:|------:|---------|-------:|------|
|     0 |     0 |      0 |       0 |     0.0 |       0 | 13:39:59.908 |  2022-02-19 |    0 | 1645303199909000000 |      0 |     0 |    true |    0.0 |    0 |
|     1 |     1 |      1 |       1 |     1.0 |       1 | 13:39:59.910 |  2022-02-19 |    1 | 1645303199911000000 |      1 |     1 |   false |    1.0 |    1 |
|     2 |     2 |      2 |       2 |     2.0 |       2 | 13:39:59.910 |  2022-02-19 |    2 | 1645303199911000000 |      2 |     2 |    true |    2.0 |    2 |
|     3 |     3 |      3 |       3 |     3.0 |       3 | 13:39:59.910 |  2022-02-19 |    3 | 1645303199911000000 |      3 |     3 |    true |    3.0 |    3 |
|     4 |     4 |      4 |       4 |     4.0 |       4 | 13:39:59.910 |  2022-02-19 |    4 | 1645303199911000000 |      4 |     4 |   false |    4.0 |    4 |
|     5 |     5 |      5 |       5 |     5.0 |       5 | 13:39:59.910 |  2022-02-19 |    5 | 1645303199911000000 |      5 |     5 |   false |    5.0 |    5 |
|     6 |     6 |      6 |       6 |     6.0 |       6 | 13:39:59.910 |  2022-02-19 |    6 | 1645303199911000000 |      6 |     6 |    true |    6.0 |    6 |
|     7 |     7 |      7 |       7 |     7.0 |       7 | 13:39:59.910 |  2022-02-19 |    7 | 1645303199911000000 |      7 |     7 |   false |    7.0 |    7 |
|     8 |     8 |      8 |       8 |     8.0 |       8 | 13:39:59.910 |  2022-02-19 |    8 | 1645303199911000000 |      8 |     8 |   false |    8.0 |    8 |
|     9 |     9 |      9 |       9 |     9.0 |       9 | 13:39:59.910 |  2022-02-19 |    9 | 1645303199911000000 |      9 |     9 |    true |    9.0 |    9 |

;; first-last is default with print-index-range of 20
user> (vary-meta ds assoc :print-style :first-last :print-index-range 10)
test/data/alldtypes.arrow-feather-compressed [1000 15]:

| uints | longs | ubytes | strings | doubles | ushorts |  local_times | local_dates | ints |            instants | shorts | bytes | boolean | floats | text |
|------:|------:|-------:|---------|--------:|--------:|--------------|-------------|-----:|--------------------:|-------:|------:|---------|-------:|------|
|     0 |     0 |      0 |       0 |     0.0 |       0 | 13:39:59.908 |  2022-02-19 |    0 | 1645303199909000000 |      0 |     0 |    true |    0.0 |    0 |
|     1 |     1 |      1 |       1 |     1.0 |       1 | 13:39:59.910 |  2022-02-19 |    1 | 1645303199911000000 |      1 |     1 |   false |    1.0 |    1 |
|     2 |     2 |      2 |       2 |     2.0 |       2 | 13:39:59.910 |  2022-02-19 |    2 | 1645303199911000000 |      2 |     2 |    true |    2.0 |    2 |
|     3 |     3 |      3 |       3 |     3.0 |       3 | 13:39:59.910 |  2022-02-19 |    3 | 1645303199911000000 |      3 |     3 |    true |    3.0 |    3 |
|     4 |     4 |      4 |       4 |     4.0 |       4 | 13:39:59.910 |  2022-02-19 |    4 | 1645303199911000000 |      4 |     4 |   false |    4.0 |    4 |
|   ... |   ... |    ... |     ... |     ... |     ... |          ... |         ... |  ... |                 ... |    ... |   ... |     ... |    ... |  ... |
|   994 |   994 |    226 |     994 |   994.0 |     994 | 13:39:59.923 |  2022-02-19 |  994 | 1645303199916000000 |    994 |   -30 |         |  994.0 |  994 |
|   995 |   995 |    227 |     995 |   995.0 |     995 | 13:39:59.923 |  2022-02-19 |  995 | 1645303199916000000 |    995 |   -29 |         |  995.0 |  995 |
|   996 |   996 |    228 |     996 |   996.0 |     996 | 13:39:59.923 |  2022-02-19 |  996 | 1645303199916000000 |    996 |   -28 |         |  996.0 |  996 |
|   997 |   997 |    229 |     997 |   997.0 |     997 | 13:39:59.923 |  2022-02-19 |  997 | 1645303199916000000 |    997 |   -27 |         |  997.0 |  997 |
|   998 |   998 |    230 |     998 |   998.0 |     998 | 13:39:59.923 |  2022-02-19 |  998 | 1645303199916000000 |    998 |   -26 |         |  998.0 |  998 |
|   999 |   999 |    231 |     999 |   999.0 |     999 | 13:39:59.923 |  2022-02-19 |  999 | 1645303199916000000 |    999 |   -25 |         |  999.0 |  999 |
```

Example of conservative printing:

```clojure
tech.ml.dataset.github-test> (def ds (with-meta ds
                                       (assoc (meta ds)
                                              :print-column-max-width 25
                                              :print-line-policy :single)))
```"
  ([dataset]
   (dataset-data->str dataset {}))
  ([dataset options]
   (let [options (merge (meta dataset) options)
         {:keys [print-index-range print-line-policy
                 print-column-max-width print-column-types?
                 maximum-precision]} options
         n-rows (long (second (dtype/shape dataset)))
         index-range (or print-index-range
                         (min n-rows *default-table-row-print-length*))
         print-style (when (number? index-range) (get options :print-style :first-last))
         [index-range ellipses?] (if (number? index-range)
                                   (case print-style
                                     :first-last
                                     (if (> n-rows (long index-range))
                                       (let [start-n (quot (long index-range) 2)
                                             end-start (dec (- n-rows start-n))]
                                         [(vec (concat (range start-n)
                                                       (range end-start n-rows)))
                                          true])
                                       [(range n-rows) false])
                                     :first
                                     [(range (min index-range n-rows)) false]
                                     :last
                                     [(range (max 0 (- n-rows (long index-range))) n-rows) false])
                                   [index-range false])
         line-policy (or print-line-policy *default-print-line-policy*)
         column-width (or print-column-max-width *default-print-column-max-width*)
         column-types? (or print-column-types? *default-print-column-types?*)
         print-ds (if (keyword? index-range)
                    (if (identical? index-range :all)
                      dataset
                      (throw (RuntimeException. (str "Unrecognized index range keyword: "
                                                     index-range))))
                    (ds-proto/select-rows dataset
                                          (lznc/filter
                                           (hamf-fn/long-predicate
                                            idx (and (< idx n-rows)
                                                     (>= idx (- n-rows))))
                                           index-range)))
         column-names (map #(when (some? %) (.toString ^Object %)) (keys print-ds))
         column-types (map #(str (when column-types? (:datatype (meta %))))
                           (vals print-ds))
         string-columns (map #(-> (dtype/->reader %)
                                  (reader->string-lines (ds-proto/missing %)
                                                        line-policy
                                                        column-width
                                                        true
                                                        maximum-precision)
                                  ;;Do the conversion to string once.
                                  (dtype/clone)
                                  (dtype/->reader))
                             (vals print-ds))
         string-columns (if ellipses?
                          (let [insert-pos (quot (dtype/ecount index-range) 2)]
                            (->> string-columns
                                 (map (fn [str-col]
                                        (vec (concat (take insert-pos str-col)
                                                     [["..."]]
                                                     (drop insert-pos str-col)))))))
                          string-columns)
         n-rows (long (second (dtype/shape print-ds)))
         n-rows (long (if ellipses?
                        (inc n-rows)
                        n-rows))
         row-heights (ArrayList.)
         _ (.addAll row-heights (repeat n-rows 1))
         column-widths
         (->> string-columns
              (map (fn [coltype colname coldata]
                     (->> coldata
                          (map-indexed
                           (fn [row-idx lines]
                             ;;Side effecting record row height.
                             (.set row-heights (int row-idx)
                                   (max (int (.get row-heights row-idx))
                                        (count lines)))
                             (apply max 0 (map count lines))))
                          (apply max (count coltype) (count colname))))
                   column-types
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
     (when column-types? (append-line! builder (fmt-row "| " " | " " |" column-types)))
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
                             spacers (map dtype/elemwise-datatype
                                          (vals print-ds)))
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
     (let [options (merge (meta ds) options)
           elide-header? (get options :elide-header?)
           header (if elide-header?
                    ""
                    (format "%s %s:\n\n"
                            (ds-proto/dataset-name ds)
                            (vec (reverse (dtype/shape ds)))))]

       (str header (dataset-data->str ds options)))))
  ([ds]
   (dataset->str ds {})))


(defn print-range
  "Convenience function to set the number of rows to print.\n
   Defaults to (range *default-table-row-print-length*) - one of:
   - n - prints the first n rows
   - range - prints the rows at positions corresponding to the range
   - `:all` - prints all the rows in a dataset"
  [dataset index-range]
  (-> dataset
      (vary-meta assoc :print-index-range index-range)))


(defn print-policy
  "Convenience function to vary printing behavior.\n
   Defaults to `:repl` - one of:
   - `:repl` - multiline table - default nice printing for repl
   - `:markdown` - lines delimited by <br>
   - `:single` - Only print first line"
  [dataset line-policy]
  (-> dataset
      (vary-meta assoc :print-line-policy line-policy)))


(defn print-width
  "Convenience function to set the max width of a column when printing."
  [dataset column-width]
  (-> dataset
      (vary-meta assoc :print-column-max-width column-width)))


(defn print-types
  "Convenience function to show/hide column types"
  [dataset column-types]
  (-> dataset
      (vary-meta assoc :print-column-types? column-types)))
