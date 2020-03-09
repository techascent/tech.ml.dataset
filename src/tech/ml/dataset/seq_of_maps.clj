(ns tech.ml.dataset.seq-of-maps
  "Helper functions for dealing with a sequence of maps.")


(defn- in-range
  [[l-min l-max] [r-min r-max]]
  (if (integer? r-min)
    (let [l-min (long l-min)
          l-max (long l-max)
          r-min (long r-min)
          r-max (long r-max)]
      (if (and (>= l-min r-min)
               (<= l-min r-max)
               (>= l-max r-min)
               (<= l-max r-max))
        true
        false))
    (let [l-min (double l-min)
          l-max (double l-max)
          r-min (double r-min)
          r-max (double r-max)]
      (if (and (>= l-min r-min)
               (<= l-min r-max)
               (>= l-max r-min)
               (<= l-max r-max))
        true
        false))))


(defn autoscan-map-seq
  "Scan the first n entries of a sequence of maps to derive the datatypes."
  [map-seq-dataset {:keys [scan-depth]
                    :as options}]
  (->> (take 100 map-seq-dataset)
       (reduce (fn [defs next-row]
                 (reduce
                  (fn [defs [row-name row-val]]
                    (let [{:keys [datatype min-val max-val] :as existing}
                          (get defs row-name {:name row-name})]
                      (assoc defs row-name
                             (cond
                               (nil? row-val)
                               existing

                               (keyword? row-val)
                               (if (#{:integer :float} datatype)
                                 (assoc existing :datatype :string)
                                 (assoc existing :datatype (or datatype :keyword)))

                               (string? row-val)
                               (assoc existing :datatype :string)

                               (number? row-val)
                               (if (#{:string :object} datatype)
                                 (assoc existing :datatype :object)
                                 (assoc existing
                                        :min-val (if min-val
                                                   (apply min [min-val row-val])
                                                   row-val)
                                        :max-val (if max-val
                                                   (apply max [max-val row-val])
                                                   row-val)
                                        :datatype (if (integer? row-val)
                                                    (if (= datatype :boolean)
                                                      :boolean
                                                      (or datatype :integer))
                                                    :float)))
                               (boolean? row-val)
                               (if (#{:string :object} datatype)
                                 (assoc existing :datatype :object)
                                 (assoc existing
                                        :datatype
                                        (if (#{:integer :float} datatype)
                                          datatype
                                          :boolean)))))))
                  defs
                  next-row))
               {})
       ((fn [def-map]
          (->> def-map
               (map (fn [[defname {:keys [datatype min-val max-val] :as definition}]]
                      {:name defname
                       :datatype
                       (if (nil? datatype)
                         :string
                         (case datatype
                           :integer
                           (let [val-range [min-val max-val]]
                             (cond
                               (in-range val-range [Short/MIN_VALUE
                                                    Short/MAX_VALUE])
                               :int16
                               (in-range val-range [Integer/MIN_VALUE
                                                    Integer/MAX_VALUE])
                               :int32
                               :else
                               :int64))
                           :float (let [val-range [min-val max-val]]
                                    (cond
                                      (in-range val-range [(- Float/MAX_VALUE)
                                                           Float/MAX_VALUE])
                                      :float32
                                      :else
                                      :float64))
                           :string :string
                           :boolean :boolean
                           ;;Let other people sort it out.
                           :keyword :keyword
                           :object :object))})))))))



(defn ->flyweight
  "Convert dataset to seq-of-maps dataset.  Flag indicates if errors should be thrown
  on missing values or if nil should be inserted in the map.  IF a label map is passed
  in then for the columns that are present in the label map a reverse mapping is done
  such that the flyweight maps contain the labels and not their encoded values."
  [dataset & {:keys [column-name-seq
                     error-on-missing-values?
                     number->string?]
              :or {error-on-missing-values? true}}]
  (let [label-map (when number->string?
                    (dataset-label-map dataset))
        target-columns-and-vals
        (->> (or column-name-seq
                 (->> (columns dataset)
                      (map ds-col/column-name)
                      ((fn [colname-seq]
                         (if number->string?
                           (reduce-column-names dataset colname-seq)
                           colname-seq)))))
             (map (fn [colname]
                    {:column-name colname
                     :column-values
                     (if (contains? label-map colname)
                       (let [retval
                             (categorical/column-values->categorical
                              dataset colname label-map)]
                         retval)
                       (let [current-column (column dataset colname)]
                         (when (and error-on-missing-values?
                                    (not= 0 (count (ds-col/missing current-column))))
                           (throw (ex-info (format "Column %s has missing values"
                                                   (ds-col/column-name current-column))
                                           {})))
                         (dtype/->reader current-column)))})))]
    ;;Transpose the sequence of columns into a sequence of rows
    (->> target-columns-and-vals
         (map :column-values)
         (apply interleave)
         (partition (count target-columns-and-vals))
         ;;Move to flyweight
         (map zipmap
              (repeat (map :column-name target-columns-and-vals))))))
