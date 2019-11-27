(ns tech.viz.desktop
  "Namespace that you can't require in jdk headless configurations.")



(defn ->clipboard
  "Copy string to the desktop clipboard."
  [s]
  (-> (.getSystemClipboard (java.awt.Toolkit/getDefaultToolkit))
      (.setContents (java.awt.datatransfer.StringSelection. s) nil)))
