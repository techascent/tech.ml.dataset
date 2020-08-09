(defproject techascent/tech.ml.dataset "4.01"
  :description "Dataset and ETL pipeline for machine learning"
  :url "http://github.com/techascent/tech.ml.dataset"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure              "1.10.1"]
                 [camel-snake-kebab                "0.4.0"]
                 [techascent/tech.datatype         "5.12"]
                 [techascent/tech.io               "3.17"
                  :exclusions [org.apache.commons/commons-compress]]
                 [com.univocity/univocity-parsers  "2.7.5"]
                 [org.apache.poi/poi-ooxml         "4.1.2"]
                 [org.dhatim/fastexcel-reader      "0.10.12"
                  :exclusions [org.apache.poi/poi-ooxml]]
                 [com.github.haifengl/smile-core   "2.5.0"
                  :exclusions [org.slf4j/slf4j-api]]
                 [com.github.haifengl/smile-io     "2.5.0"
                  :exclusions [com.github.haifengl/smile-core
                               org.slf4j/slf4j-api]]
                 [com.github.haifengl/smile-nlp    "2.5.0"
                  :exclusions [com.github.haifengl/smile-core
                               org.slf4j/slf4j-api]]
                 [ch.qos.logback/logback-classic   "1.2.3"]
                 [http-kit                         "2.3.0"]
                 ;;Things require guava, so we may as well have latest version
                 [com.google.guava/guava "28.0-jre"]]
  :profiles {:dev
             {:dependencies [[criterium "0.4.5"]
                             [http-kit "2.3.0"]
                             [techascent/tech.viz "0.3"]
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
                              :exclusions [commons-codec]]]}}
  :jvm-opts ["-Djdk.attach.allowAttachSelf=true"]
  :test-selectors {:travis (complement :travis-broken)}
  :java-source-paths ["java"])
