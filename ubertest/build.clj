(ns build
  (:require [clojure.tools.build.api :as b]
            [clojure.edn :as edn])
  (:refer-clojure :exclude [compile]))

(def basis (b/create-basis {:project "deps.edn"}))
(def uber-file "target/ubertest.jar")
(def class-dir "target/classes")


(defn clean [_]
  (b/delete {:path "target"}))


(defn ubertest [_]
  (clean nil)
  (b/compile-clj {:basis basis
                  :src-dirs ["src"]
                  :class-dir class-dir})
  (b/uber {:class-dir class-dir
           :uber-file uber-file
           :basis basis
           :main 'ubertest.main}))
