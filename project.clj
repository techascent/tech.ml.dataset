(defproject techascent/tech.ml.dataset "2.0-beta-31-SNAPSHOT"
  :description "Dataset and ETL pipeline for machine learning"
  :url "http://github.com/techascent/tech.ml.dataset"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :plugins [[lein-tools-deps "0.4.1"]]
  :middleware [lein-tools-deps.plugin/resolve-dependencies-with-deps-edn]
  :lein-tools-deps/config {:config-files [:install :user :project]}
  :profiles {:dev {:lein-tools-deps/config {:resolve-aliases [:test]}}
             :uberjar {:aot :all}}
  :java-source-paths ["java"])
