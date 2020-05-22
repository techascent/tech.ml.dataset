(ns tech.ml.dataset.print
  (:require [clojure.pprint :as pp]
            [tech.ml.protocols.dataset :as ds-proto]
            [tech.ml.dataset.column :as ds-col]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.pprint :as dtype-pp]
            [tech.v2.datatype.datetime :as dtype-dt]
            [clojure.string :as str])
  (:import [tech.v2.datatype ObjectReader]
           [java.util List HashMap Collections ArrayList]
           [tech.ml.dataset FastStruct]
           [clojure.lang PersistentStructMap$Def
            PersistentVector]
           [org.roaringbitmap RoaringBitmap]))

(set! *warn-on-reflection* true)

(def ^:dynamic *default-table-row-print-length* 25)


(defn- dataset->readers
  ^List [dataset {:keys [all-printable-columns?
                         missing-nil?]
                  :or {missing-nil? true}
                  :as _options}]
  (->> (ds-proto/columns dataset)
       (mapv (fn [coldata]
               (let [col-reader (dtype/->reader coldata)
                     ^RoaringBitmap missing (dtype/as-roaring-bitmap
                                             (ds-col/missing coldata))
                     col-rdr (typecast/datatype->reader
                               :object
                               (if (or all-printable-columns?
                                       (dtype-dt/packed-datatype?
                                        (dtype/get-datatype col-reader)))
                                 (dtype-pp/reader-converter col-reader)
                                 col-reader))]
                 (if missing-nil?
                   (reify ObjectReader
                     (lsize [rdr] (.size col-rdr))
                     (read [rdr idx]
                       (when-not (.contains missing idx)
                         (.read col-rdr idx))))
                   col-rdr))))))


(defn value-reader
  "Return a reader that produces a vector of column values per index.
  Options:
  :all-printable-columns? - When true, all columns are run through the datatype
    library's reader-converter multimethod.  This can change the value of a column
    such that it prints nicely but may not be an exact substitution for the column
    value.
  :missing-nil? - Substitute nil in for missing values to make missing value
     detection downstream to be column datatype independent."
  (^ObjectReader [dataset options]
   (let [readers (dataset->readers dataset options)
         n-rows (long (second (dtype/shape dataset)))
         n-cols (long (first (dtype/shape dataset)))]
     (reify ObjectReader
       (lsize [rdr] n-rows)
       (read [rdr idx]
         (reify ObjectReader
           (lsize [inner-rdr] n-cols)
           (read [inner-rd inner-idx]
             ;;confusing because there is an implied transpose
             (.get ^List (.get readers inner-idx)
                   idx)))))))
  (^ObjectReader [dataset]
   (value-reader dataset {})))


(defn mapseq-reader
  "Return a reader that produces a map of column-name->column-value
  Options:
  :all-printable-columns? - When true, all columns are run through the datatype
    library's reader-converter multimethod.  This can change the value of a column
    such that it prints nicely but may not be an exact substitution for the column
    value.
  :missing-nil? - Substitute nil in for missing values to make missing value
     detection downstream to be column datatype independent.
  "
  (^ObjectReader [dataset options]
   (let [colnamemap (HashMap.)
         _ (doseq [[c-name c-idx] (->> (ds-proto/column-names dataset)
                                      (map-indexed #(vector %2 (int %1))))]
             (.put colnamemap c-name c-idx))
         colnamemap (Collections/unmodifiableMap colnamemap)
         readers (value-reader dataset options)]
     (reify ObjectReader
       (lsize [rdr] (.lsize readers))
       (read [rdr idx]
         (FastStruct. colnamemap (.read readers idx))))))
  (^ObjectReader [dataset]
   (mapseq-reader dataset {})))


(defn- reader->string-lines
  [reader-data ^RoaringBitmap missing
   line-policy]
  (dtype/object-reader
   (dtype/ecount reader-data)
   #(if (.contains missing (int %))
      nil
      (let [lines
            (str/split-lines (.toString ^Object (reader-data %)))]
        (case line-policy
          :single
          [(first lines)]
          :markdown
          [(str/join "<br>" lines)]
          :repl
          lines)))))


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


(def ^:dynamic *default-dataset-line-policy* :repl)


(defn dataset->str
  ([dataset]
   (dataset->str dataset {}))
  ([dataset {:keys [column-names index-range
                    line-policy]
             :or {column-names :all}}]
   (let [index-range (or index-range
                         (range
                          (min (second (dtype/shape dataset))
                               *default-table-row-print-length*)))
         line-policy (or line-policy
                         *default-dataset-line-policy*)
         print-ds (ds-proto/select dataset column-names index-range)
         column-names (map #(.toString ^Object %)
                           (ds-proto/column-names print-ds))
         string-columns (map #(-> (dtype/->reader %)
                                  (dtype-pp/reader-converter)
                                  (reader->string-lines (ds-col/missing %)
                                                        line-policy)
                                  ;;Do the conversion to string once.
                                  (dtype/clone)
                                  (dtype/->reader))
                             print-ds)
         n-rows (long (second (dtype/shape print-ds)))
         row-heights (ArrayList.)
         _ (.addAll row-heights (repeat n-rows 0))
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
                             (apply max (count colname) (map count lines))))
                          (apply max)))
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
     (append-line! builder (fmt-row "|-" "-|-" "-|" spacers))
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
