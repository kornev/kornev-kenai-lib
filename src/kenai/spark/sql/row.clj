(ns kenai.spark.sql.row
  (:require [kenai.scala.collection :refer (->clj-seq)
                                    :rename {->clj-seq seq-scala->clj}])
  (:import (org.apache.spark.sql Row)))

(defn ->seq
  "Return a Scala Seq representing the row. Elements are placed in the same order in the Seq."
  [^Row row]
  (seq-scala->clj (. row toSeq)))
