(ns tech.v3.dataset.unpack-larray
  (:require [clojure.java.classpath :as cp]
            [tech.v3.io :as io]
            [clojure.tools.logging :as log])
  (:import  [xerial.larray.impl OSInfo]
            [java.util.jar JarFile JarEntry]
            [java.nio.file Files Paths]
            [java.nio.file.attribute PosixFilePermissions])
  (:gen-class))


(def larray-os-folder (OSInfo/getNativeLibFolderPathForCurrentOS))

(def larray-libname (System/mapLibraryName "larray"))


(defn copy-jar-resource
  [os-folder libname]
  (if-let [[^JarFile jf ^JarEntry entry]
             (->> (cp/classpath-jarfiles)
                  (mapcat #(map vector (repeat %) (iterator-seq (.entries ^JarFile %))))
                  (filter (fn [[^JarFile _jf ^JarEntry entry]]
                            (and
                             (.endsWith (.getName entry) libname)
                             (.contains (.getName entry) os-folder))))
                  (first))]
    (let [fname (format "resources/%s" libname)]
      (log/infof "Copying shared library - os-path: %s, libname: %s output: %s"
                 os-folder libname fname)
      (io/copy (.getInputStream jf entry) (str "file://" fname))
      (Files/setPosixFilePermissions (Paths/get fname (into-array String []))
                                     (PosixFilePermissions/fromString "rwxr-x---")))))


(defn -main
  [& args]
  (copy-jar-resource larray-os-folder larray-libname))
