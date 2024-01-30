(ns tech.v3.libs.tribuo-test
  (:require [tech.v3.libs.tribuo :as sut]
            [clojure.test :as t]))


(t/deftest tribuo-trainer

  (let [config-components
        [{:name "trainer"
          :type "org.tribuo.classification.dtree.CARTClassificationTrainer"
          :properties {:maxDepth "6"
                       :seed "12345"
                       :fractionFeaturesInSplit "0.5"}}]
        trainer (sut/tribuo-trainer config-components "trainer")]

    (t/is (= "class org.tribuo.classification.dtree.CARTClassificationTrainer"
             (str (class trainer))))))
