(ns tech.v3.libs.arrow.allocator
  "Defines a publicly available Arrow allocator."
  (:import [org.apache.arrow.memory RootAllocator BufferAllocator]))


(defonce ^{:doc "Allocator binding.  Must be either an instance of BaseAllocator
or a delay which resolves to one."
           :dynamic true} *allocator* (delay (RootAllocator. Long/MAX_VALUE)))


(defn allocator
  "Retive an instance of the arrow root allocator.  If none is bound then a static
  instance that is initialized upon first use is returned."
  (^BufferAllocator []
   (let [alloc-deref @*allocator*]
     (cond
       (instance? clojure.lang.IDeref alloc-deref)
       @alloc-deref
       (instance? BufferAllocator alloc-deref)
       alloc-deref
       :else
       (throw (Exception. "No allocator provided.  See `with-allocator`")))))
  (^BufferAllocator [options]
   (or (:allocator options) (allocator))))


(defmacro with-allocator
  "Bind a new allocator.  alloc* must be either an instance of
  org.apache.arrow.memory.BaseAllocator or an instance of IDeref that resolves to an
  instance of BaseAllocator."
  [alloc* & body]
  `(with-bindings {#'*allocator* ~alloc*}
     ~@body))
