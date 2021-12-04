(ns tech.v3.datatype.jvm-map)


(defmacro bi-consumer
  [karg varg code]
  `(let [~karg 1
         ~varg 2]
     ~@code))


(defmacro bi-function
  [karg varg code]
  `(let [~karg 1
         ~varg 2]
     ~@code))


(defmacro function
  [karg code]
  `(let [~karg 1]
     ~@code))
