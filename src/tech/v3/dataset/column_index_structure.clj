(ns tech.v3.dataset.column-index-structure
  (:require [tech.v3.protocols.column :as col-proto]))

(defn select-from-index
  "Select a subset of the index as specified by the `mode` and
  `selection-spec`. Each index-structure type supports different
  modes. By default, tech.ml.dataset supports two index types:
  `java.util.TreeMap` for non-categorical data, and `java.util.LinkedHashMap`
  for categorical data.

  Mode support for these types is as follows:

  `java.util.TreeMap` - supports `::slice` and `::pick`.
  `java.util.LinkedHashMap - support `::pick`

  Usage by mode:

  :slice
  (select-from-index :slice {:from 2 :to 5})
  (select-from-index :slice {:from 2 :from-inclusive? true :to 5 :to-inclusive? false})

  :pick
  (select-from-index :pick [:a :c :e])

  Note: Other index structures defined by other libraries may define other modes, so
        consult documentation."
  [index-structure mode selection-spec]
  (col-proto/select-from-index index-structure mode selection-spec))
