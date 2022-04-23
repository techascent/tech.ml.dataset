(defproject ubertest "1.00"
  :dependencies [[org.clojure/clojure "1.10.3" :scope "provided"]
                 [techascent/tech.ml.dataset "6.084"]]
  :profiles
  {:uberjar {:aot :all
             :main ubertest.main
             :jvm-opts ["-Dclojure.compiler.direct-linking=true"]
             :uberjar-name "dataset.jar"}})
