(ns kenai.scala.tuple
  (:require [kenai.scala.collection :as scala-collection])
  (:import (scala Product)))

(defn ->clj-vec
  "Converts a Scala Product type (for example, any instance of any Tuple class) to a Clojure vector."
  [^Product x]
  (-> x .productIterator .toSeq scala-collection/->clj-seq vec))
