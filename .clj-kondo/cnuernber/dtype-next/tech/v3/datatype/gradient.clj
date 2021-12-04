(ns tech.v3.datatype.gradient)


(defmacro append-diff
  [rtype n-elems read-fn cast-fn append reader]
  `(. ~read-fn ~reader))

(defmacro prepend-diff
  [rtype n-elems read-fn cast-fn append reader]
  `(. ~read-fn ~reader))

(defmacro basic-diff
  [rtype n-elems read-fn reader]
  `(. ~read-fn ~reader))
