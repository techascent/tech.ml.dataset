(defproject techascent/tech.ml.dataset "5.0.0-SNAPSHOT"
  :description "Clojure data processing system"
  :url "http://github.com/techascent/tech.ml.dataset"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure              "1.10.2-alpha1"]
                 [camel-snake-kebab                "0.4.0"]
                 [cnuernber/dtype-next             "0.4.7"]
                 [techascent/tech.io               "3.19"
                  :exclusions [org.apache.commons/commons-compress]]
                 [com.univocity/univocity-parsers  "2.7.5"]
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
                 [com.google.guava/guava "28.0-jre"]]
  :test-selectors {:travis (complement :travis-broken)}
  :profiles {:dev
             {:dependencies [[criterium "0.4.5"]
                             [http-kit "2.3.0"]
                             [org.bytedeco/openblas "0.3.10-1.5.4"]
                             [org.bytedeco/openblas-platform "0.3.10-1.5.4"]
                             ;; [uncomplicate/neanderthal "0.35.0"]
                             [com.clojure-goes-fast/clj-memory-meter "0.1.0"]
                             [org.apache.parquet/parquet-hadoop "1.10.1"]
                             [org.apache.hadoop/hadoop-common
                              "3.1.1"
                              ;;We use logback-classic.
                              :exclusions [org.slf4j/slf4j-log4j12
                                           log4j
                                           com.google.guava/guava
                                           commons-codec
                                           commons-logging
                                           com.google.code.findbugs/jsr305
                                           com.fasterxml.jackson.core/jackson-databind]]
                             [org.apache.arrow/arrow-memory-netty "1.0.0"]
                             [org.apache.arrow/arrow-memory-core "1.0.0"]
                             [org.apache.arrow/arrow-vector "1.0.0"
                              :exclusions [commons-codec]]]
              :test-paths ["test" "neanderthal"]}
             ;;No neanderthal on travis
             :travis {:dependencies [[criterium "0.4.5"]
                                     [http-kit "2.3.0"]
                                     [com.clojure-goes-fast/clj-memory-meter "0.1.0"]
                                     [org.apache.parquet/parquet-hadoop "1.10.1"]
                                     [org.apache.hadoop/hadoop-common
                                      "3.1.1"
                                      ;;We use logback-classic.
                                      :exclusions [org.slf4j/slf4j-log4j12
                                                   log4j
                                                   com.google.guava/guava
                                                   commons-codec
                                                   commons-logging
                                                   com.google.code.findbugs/jsr305
                                                   com.fasterxml.jackson.core/jackson-databind]]
                                     [org.apache.arrow/arrow-memory-netty "1.0.0"]
                                     [org.apache.arrow/arrow-memory-core "1.0.0"]
                                     [org.apache.arrow/arrow-vector "1.0.0"
                                      :exclusions [commons-codec]]]}
             :uberjar {:aot [tech.v3.dataset.column tech.io]
                       :source-paths ["src"]
                       :jvm-opts ["-Dclojure.compiler.direct-linking=true"]}}
  :jvm-opts ["-Djdk.attach.allowAttachSelf=true"]
  :java-source-paths ["java"])
