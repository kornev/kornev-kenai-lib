(ns kenai.sql.row
  (:import (org.apache.spark.sql Row)))

(defn get-list
  ^java.util.List [^Row row col-name]
  (->> (. row fieldIndex (name col-name))
       (. row getList)))
