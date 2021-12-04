(ns taoensso.nippy)


(defmacro extend-freeze
  [dt _kwd argvec & code]
  (let [[buf-var out-var] argvec]
    `(let [~buf-var 1
           ~out-var 2]
       (type ~dt)
       ~@code)))


(defmacro extend-thaw
  [_kwd argvec & code]
  (let [invar (first argvec)]
    `(let [~invar 1]
       ~@code)))
