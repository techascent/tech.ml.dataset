(defproject techascent/tech.ml.dataset "6.047-SNAPSHOT"
  :description "Clojure high performance data processing system"
  :url "http://github.com/techascent/tech.ml.dataset"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure              "1.10.3" :scope "provided"]
                 [cnuernber/dtype-next             "8.054"]
                 [techascent/tech.io               "4.09"
                  :exclusions [org.apache.commons/commons-compress]]
                 [com.univocity/univocity-parsers  "2.9.0"]
                 [org.apache.poi/poi-ooxml         "5.1.0"
                  :exclusions [commons-codec]]
                 [org.dhatim/fastexcel-reader      "0.12.8"
                  :exclusions [org.apache.poi/poi-ooxml]]
                 [com.github.haifengl/smile-core   "2.6.0"
                  :exclusions [org.slf4j/slf4j-api]]
                 [com.github.haifengl/smile-io     "2.6.0"
                  :exclusions [com.github.haifengl/smile-core
                               org.slf4j/slf4j-api]]
                 [com.github.haifengl/smile-nlp    "2.6.0"
                  :exclusions [com.github.haifengl/smile-core
                               org.slf4j/slf4j-api]]
                 ;;In your projects you may want to use exclusions.
                 [ch.qos.logback/logback-classic "1.2.3"
                  :exclusions [org.slf4j/slf4j-api]]
                 ;;Version of slf4j hand chosen to be a middle ground between projects.
                 [org.slf4j/slf4j-api "1.7.30"]

                 ;;High performance probabilistic stats
                 [com.techascent/t-digest "4.0-pre-release-1"]
                 [org.apache.datasketches/datasketches-java "2.0.0"]

                 ;;provided scope
                 [org.bytedeco/openblas "0.3.10-1.5.4" :scope "provided"]
                 [org.bytedeco/openblas-platform "0.3.10-1.5.4" :scope "provided"]
                 [org.apache.arrow/arrow-vector "6.0.0"
                  :exclusions [commons-codec
                               com.fasterxml.jackson.core/jackson-core
                               com.fasterxml.jackson.core/jackson-annotations
                               com.fasterxml.jackson.core/jackson-databind
                               org.slf4j/slf4j-api]
                  :scope "provided"]
                 [uncomplicate/neanderthal "0.35.0" :scope "provided"]

                 ;;Geni dependencies
                 [zero.one/geni "0.0.34" :scope "provided"
                  :exclusions [commons-codec]]
                 [org.apache.spark/spark-avro_2.12 "3.0.1" :scope "provided"]
                 ;;Remove spark-core's dependency on log4j12, they use
                 ;;slf4j anyway.
                 [org.apache.spark/spark-core_2.12 "3.0.1"
                  :scope "provided"
                  :exclusions [org.slf4j/slf4j-log4j12]]
                 [org.apache.spark/spark-hive_2.12 "3.0.1" :scope "provided"]
                 [org.apache.spark/spark-mllib_2.12 "3.0.1" :scope "provided"]
                 [org.apache.spark/spark-sql_2.12 "3.0.1" :scope "provided"]
                 [org.apache.spark/spark-streaming_2.12 "3.0.1" :scope "provided"]]
  :test-selectors {:travis (complement :travis-broken)}
  :profiles {:dev
             {:dependencies [[criterium "0.4.5"]
                             [http-kit "2.3.0"]
                             [com.clojure-goes-fast/clj-memory-meter "0.1.0"]]
              :source-paths ["src"]
              :resource-paths ["dev-resources"]
              :test-paths ["test" "neanderthal"]}
             :jdk-17 {:jvm-opts ["--add-modules" "jdk.incubator.foreign,jdk.incubator.vector"
                                 "--enable-native-access=ALL-UNNAMED"]}
             :codegen
             {:source-paths ["src" "dev"]}
             :codox
             {:dependencies [[codox-theme-rdash "0.1.2"]
                             [com.cnuernber/codox "1.000"]]
              :codox {:project {:name "tech.ml.dataset"}
                      :metadata {:doc/format :markdown}
                      :themes [:rdash]
                      :source-paths ["src"]
                      :output-path "docs"
                      :doc-paths ["topics"]
                      :source-uri "https://github.com/techascent/tech.ml.dataset/blob/master/{filepath}#L{line}"
                      :namespaces [tech.v3.dataset
                                   tech.v3.dataset.math
                                   tech.v3.dataset.tensor
                                   tech.v3.dataset.join
                                   tech.v3.dataset.modelling
                                   tech.v3.dataset.column
                                   tech.v3.dataset.clipboard
                                   tech.v3.dataset.neanderthal
                                   tech.v3.dataset.metamorph
                                   tech.v3.dataset.categorical
                                   tech.v3.dataset.rolling
                                   tech.v3.dataset.reductions
                                   tech.v3.dataset.reductions.apache-data-sketch
                                   tech.v3.dataset.column-filters
                                   tech.v3.dataset.io.datetime
                                   tech.v3.dataset.io.univocity
                                   tech.v3.dataset.io.string-row-parser
                                   tech.v3.dataset.print
                                   tech.v3.libs.smile.data
                                   tech.v3.libs.poi
                                   tech.v3.libs.parquet
                                   tech.v3.libs.fastexcel
                                   tech.v3.libs.arrow
                                   tech.v3.libs.guava.cache]}}
             ;;No neanderthal on travis
             :uberjar {:aot [tech.v3.dataset.main]
                       :main tech.v3.dataset.main
                       :source-paths ["src" "graal-native"]
                       :jvm-opts ["-Dclojure.compiler.direct-linking=true"
                                  "-Dtech.v3.datatype.graal-native=true"]
                       :uberjar-name "dataset.jar"}
             :larray {:source-paths ["graal-native"]}}
  :jvm-opts ["-Djdk.attach.allowAttachSelf=true"]
  :java-source-paths ["java"]
  :aliases {"codox" ["with-profile" "codox,dev" "run" "-m" "codox.main/-main"]
            "codegen" ["with-profile" "codegen,dev" "run" "-m" "tech.v3.dataset.codegen/-main"]})
