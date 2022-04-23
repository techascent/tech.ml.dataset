(defproject ubertest "1.00"
  :dependencies [[org.clojure/clojure "1.10.3" :scope "provided"]
                 [techascent/tech.ml.dataset "6.084"]]
  :profiles
  ;; You really should just aot your main namespace and not everything.
  ;; But I have reports from the field of oddness when someone aot's everything.
  {:uberjar {:aot :all
             :main ubertest.main
             :jvm-opts ["-Dclojure.compiler.direct-linking=true"]
             :uberjar-name "dataset.jar"}})
