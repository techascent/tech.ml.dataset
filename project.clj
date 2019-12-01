(defproject techascent/tech.ml.dataset "1.59"
  :description "Dataset and ETL pipeline for machine learning"
  :url "http://github.com/techascent/tech.ml.dataset"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :plugins [[lein-tools-deps "0.4.1"]]
  :middleware [lein-tools-deps.plugin/resolve-dependencies-with-deps-edn]
  :lein-tools-deps/config {:config-files [:install :user :project]}
  :profiles {:uberjar {:aot :all}})
