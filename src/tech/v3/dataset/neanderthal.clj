(ns tech.v3.dataset.neanderthal
  "Conversion of a dataset to/from a neanderthal dense matrix"
  (:require [uncomplicate.neanderthal.core :as n-core]
            [uncomplicate.neanderthal.native :as n-native]
            [tech.v3.libs.neanderthal]
            [tech.v3.dataset.base :as ds-base]
            [tech.v3.dataset.impl.dataset :as ds-impl]
            [tech.v3.datatype :as dtype]
            [tech.v3.tensor :as dtt]))


(defn dataset->dense
  "Convert a dataset into a dense neanderthal CPU matrix.  If the matrix
  is column-major, then potentially you can get accerated copies from the dataset
  into neanderthal."
  ([dataset neanderthal-layout datatype]
   (let [[n-cols n-rows] (dtype/shape dataset)
         retval (case datatype
                  :float64
                  (n-native/dge n-rows n-cols
                                {:layout
                                 neanderthal-layout}))
         tens (dtt/ensure-tensor retval)
         tens-cols (dtt/columns tens)]
     ;;If possible, these will be accelerated copies
     (->> (pmap (fn [tens-col ds-col]
                  (dtype/copy! ds-col tens-col))
                tens-cols
                (vals dataset))
          (dorun))
     retval))
  ([dataset neanderthal-layout]
   (dataset->dense dataset neanderthal-layout :float64))
  ([dataset]
   (dataset->dense dataset :column :float64)))


(defn dense->dataset
  "Given a neanderthal matrix, convert its columns into the columns of a
  tech.v3.dataset.  This does the conversion in-place.  If you would like to copy
  the neanderthal matrix into JVM arrays, then after method use dtype/clone."
  [matrix]
  (->> (n-core/cols matrix)
       (map dtt/ensure-tensor)
       (ds-impl/new-dataset :neandtheral)))
