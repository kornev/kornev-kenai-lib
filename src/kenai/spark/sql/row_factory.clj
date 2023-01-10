(ns kenai.spark.sql.row-factory
  (:import (org.apache.spark.sql Row
                                 RowFactory)))

(defn create
  "Create a Row from the given arguments."
  ^Row [coll]
  (RowFactory/create (object-array coll)))
