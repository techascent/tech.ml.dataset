(defproject techascent/tech.ml.dataset "5.06-SNAPSHOT"
  :description "Clojure high performance data processing system"
  :url "http://github.com/techascent/tech.ml.dataset"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure              "1.10.2"]
                 [camel-snake-kebab                "0.4.2"]
                 [cnuernber/dtype-next             "6.27"]
                 [techascent/tech.io               "4.03"
                  :exclusions [org.apache.commons/commons-compress]]
                 [com.univocity/univocity-parsers  "2.9.0"]
                 [org.apache.poi/poi-ooxml         "4.1.2"
                  :exclusions [commons-codec]]
                 [org.dhatim/fastexcel-reader      "0.10.12"
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
                 ;;Many things require guava, so we may as well have latest version
                 [com.google.guava/guava "28.0-jre"]
                 ;;High performance probabilistic stats
                 [com.tdunning/t-digest "3.2"]
                 ;;provided scope
                 [org.bytedeco/openblas "0.3.10-1.5.4" :scope "provided"]
                 [org.bytedeco/openblas-platform "0.3.10-1.5.4" :scope "provided"]
                 [org.apache.arrow/arrow-memory-unsafe "2.0.0" :scope "provided"]
                 [org.apache.arrow/arrow-memory-core "2.0.0" :scope "provided"
                  :exclusions [org.slf4j/slf4j-api]]
                 [org.apache.arrow/arrow-vector "2.0.0"
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
              :test-paths ["test" "neanderthal"]}
             :codox
             {:dependencies [[codox-theme-rdash "0.1.2"]
                             [codox "0.10.7" :exclusions [org.ow2.asm/asm-all]]]
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
                                   tech.v3.dataset.categorical
                                   tech.v3.dataset.reductions
                                   tech.v3.dataset.column-filters
                                   tech.v3.dataset.io.datetime
                                   tech.v3.dataset.io.univocity
                                   tech.v3.dataset.io.string-row-parser
                                   tech.v3.dataset.print
                                   tech.v3.libs.smile.data
                                   tech.v3.libs.poi
                                   tech.v3.libs.parquet
                                   tech.v3.libs.fastexcel
                                   tech.v3.libs.arrow]}}
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
  :aliases {"codox" ["with-profile" "codox,dev" "run" "-m" "tech.v3.libs.lein-codox"]
            "larray" ["with-profile" "larray" "run" "-m" "tech.v3.dataset.unpack-larray"]})
