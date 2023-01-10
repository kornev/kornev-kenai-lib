(ns kenai.java.spark.function-test
  (:require [clojure.test :refer [deftest are]]
            [kenai.spark.function :as api-functions]))

(def this-ns (ns-name *ns*))

(defprotocol TestProto
  (proto-method [this])
  (get-closure [this]))

(defrecord TestRecord [example-fn]
  TestProto
  (proto-method [_] (example-fn))
  (get-closure [_] (fn inside-fn [] nil)))

(deftest resolve-namespace-references
  (are [expected-references obj] (= expected-references (api-functions/namespace-references obj))

    ;; Simple data
    #{} nil
    #{} :keyword
    #{} 5
    #{} true
    #{} "str"
    #{} 'sym

    ;; Functions
    #{this-ns}
    (fn [])

    #{this-ns 'kenai.spark.function}
    (fn []
      (api-functions/namespace-references (fn [])))

    #{this-ns 'kenai.spark.function}
    (fn []
      (let [x (api-functions/namespace-references (fn []))]
        (x)))

    #{this-ns}
    [(fn [])]

    #{this-ns}
    (list (fn []))

    #{this-ns}
    (doto (java.util.ArrayList.)
      (.add (fn [])))

    #{this-ns}
    (doto (java.util.HashMap.)
      (.put "key" (fn [])))

    #{this-ns}
    {:key (fn [])}

    #{this-ns}
    {:key {:nested (fn [])}}

    #{this-ns}
    {:key {:nested [(fn [])]}}

    ;; Record fields.
    #{this-ns 'kenai.spark.function}
    (->TestRecord
     (fn []
       (api-functions/namespace-references nil)))

    ;; Function that closes over an object invoking a protocol method.
    #{this-ns 'kenai.spark.function}
    (let [inst (->TestRecord
                (fn []
                  (api-functions/namespace-references nil)))]
      (fn [] (proto-method inst)))

    ;; Function closure defined inside a record class.
    #{this-ns}
    (let [x (->TestRecord nil)]
      (get-closure x))))
