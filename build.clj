(ns build
  (:require [clojure.tools.build.api :as b]
            [clojure.edn :as edn])
  (:refer-clojure :exclude [compile]))

(def deps-data (edn/read-string (slurp "deps.edn")))
(def codox-data (get-in deps-data [:aliases :codox :exec-args]))
(def lib (symbol (codox-data :group-id) (codox-data :artifact-id)))
(def version (codox-data :version))
(def class-dir "target/classes")
(def basis (b/create-basis {:project "deps.edn"}))
(def jar-file (format "target/%s.jar" (name lib)))
(def uber-file (format "target/uber-%s.jar" (name lib)))

(defn clean [_]
  (b/delete {:path "target"}))

(defn compile [_]
  (b/javac {:src-dirs ["java" "java_public_api"]
            :class-dir class-dir
            :basis (b/create-basis {:project "deps.edn" :aliases [:dev-m1-mac]})
            :javac-opts ["-source" "8" "-target" "8" "-Xlint:unchecked"]}))

(defn jar [_]
  ;;Assumes scripts/compile has already been run
  (b/write-pom {:class-dir class-dir
                :lib lib
                :version version
                :basis basis
                :src-dirs ["src"]
                :pom-data [[:licenses
                            [:license
                             [:name "MIT License"]
                             [:url "https://github.com/cnuernber/charred/blob/master/LICENSE"]]]]})
  (b/copy-dir {:src-dirs ["src" "resources"]
               :target-dir class-dir})
  (b/jar {:class-dir class-dir
          :jar-file jar-file}))
