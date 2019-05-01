(defproject techascent/tech.ml.dataset "1.0-alpha2"
  :description "Dataset and ETL pipeline for machine learning"
  :url "http://github.com/techascent/tech.ml.dataset"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.10.1-beta2"]
                 [camel-snake-kebab "0.4.0"]
                 [techascent/tech.datatype "4.0-alpha18"]
                 [tech.tablesaw/tablesaw-core "0.30.2"]
                 [com.github.haifengl/smile-core "1.5.2"]
                 [com.github.haifengl/smile-netlib "1.5.2"]]

  :profiles {:dev {:dependencies [[org.clojure/tools.logging "0.3.1"]
                                  [ch.qos.logback/logback-classic "1.1.3"]]}}

  :test-selectors {:default (complement :disabled)})
