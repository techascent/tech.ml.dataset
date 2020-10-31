(defproject techascent/tech.ml.dataset "5.00-alpha-19-SNAPSHOT"
  :description "Clojure high performance data processing system"
  :url "http://github.com/techascent/tech.ml.dataset"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure              "1.10.2-alpha1"]
                 [camel-snake-kebab                "0.4.0"]
                 [cnuernber/dtype-next             "6.00-alpha-14"]
                 [techascent/tech.io               "4.02"
                  :exclusions [org.apache.commons/commons-compress]]
                 [com.univocity/univocity-parsers  "2.9.0"]
                 [org.apache.poi/poi-ooxml         "4.1.2"]
                 [org.dhatim/fastexcel-reader      "0.10.12"
                  :exclusions [org.apache.poi/poi-ooxml]]
                 [com.github.haifengl/smile-core   "2.5.3"
                  :exclusions [org.slf4j/slf4j-api]]
                 [com.github.haifengl/smile-io     "2.5.3"
                  :exclusions [com.github.haifengl/smile-core
                               org.slf4j/slf4j-api]]
                 [com.github.haifengl/smile-nlp    "2.5.3"
                  :exclusions [com.github.haifengl/smile-core
                               org.slf4j/slf4j-api]]
                 [ch.qos.logback/logback-classic   "1.2.3"]
                 ;;Many things require guava, so we may as well have latest version
                 [com.google.guava/guava "28.0-jre"]
                 ;;provided scope
                 [org.bytedeco/openblas "0.3.10-1.5.4" :scope "provided"]
                 [org.bytedeco/openblas-platform "0.3.10-1.5.4" :scope "provided"]
                 [org.apache.parquet/parquet-hadoop "1.11.0" :scope "provided"]
                 [org.apache.hadoop/hadoop-common
                  "3.1.1"
                  ;;We use logback-classic.
                  :exclusions [org.slf4j/slf4j-log4j12
                               log4j
                               com.google.guava/guava
                               commons-codec
                               com.google.code.findbugs/jsr305
                               com.fasterxml.jackson.core/jackson-databind]
                  :scope "provided"]
                 [org.apache.arrow/arrow-memory-unsafe "2.0.0" :scope "provided"]
                 [org.apache.arrow/arrow-memory-core "2.0.0" :scope "provided"]
                 [org.apache.arrow/arrow-vector "2.0.0"
                  :exclusions [commons-codec] :scope "provided"]
                 [uncomplicate/neanderthal "0.35.0" :scope "provided"]
                 ]
  :test-selectors {:travis (complement :travis-broken)}
  :profiles {:dev
             {:dependencies [[criterium "0.4.5"]
                             [http-kit "2.3.0"]
                             [com.clojure-goes-fast/clj-memory-meter "0.1.0"]]
              :test-paths ["test" "neanderthal"]}
             :codox
             {:dependencies [[codox-theme-rdash "0.1.2"]]
              :plugins [[lein-codox "0.10.7"]]
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
                                   tech.v3.dataset.categorical
                                   tech.v3.dataset.reductions
                                   tech.v3.dataset.column-filters
                                   tech.v3.dataset.io.datetime
                                   tech.v3.dataset.io.univocity
                                   tech.v3.dataset.io.string-row-parser
                                   tech.v3.dataset.print
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
  :aliases {"codox" ["with-profile" "codox,dev" "codox"]
            "larray" ["with-profile" "larray" "run" "-m" "tech.v3.dataset.unpack-larray"]})
