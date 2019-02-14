(ns tech.libs.tablesaw.compute.fastutil
  (:require [tech.compute.cpu.typed-buffer :as cpu-typed-buffer])
  (:import [it.unimi.dsi.fastutil.bytes ByteArrayList]
           [it.unimi.dsi.fastutil.shorts ShortArrayList]
           [it.unimi.dsi.fastutil.ints IntArrayList]
           [it.unimi.dsi.fastutil.longs LongArrayList]
           [it.unimi.dsi.fastutil.floats FloatArrayList]
           [it.unimi.dsi.fastutil.doubles DoubleArrayList]))



(cpu-typed-buffer/generic-extend-java-type ByteArrayList)
(cpu-typed-buffer/generic-extend-java-type ShortArrayList)
(cpu-typed-buffer/generic-extend-java-type IntArrayList)
(cpu-typed-buffer/generic-extend-java-type LongArrayList)
(cpu-typed-buffer/generic-extend-java-type FloatArrayList)
(cpu-typed-buffer/generic-extend-java-type DoubleArrayList)
