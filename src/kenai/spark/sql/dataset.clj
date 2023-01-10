(ns kenai.spark.sql.dataset
  (:refer-clojure :exclude [filter])
  (:import (org.apache.spark.api.java JavaRDD)
           (org.apache.spark.sql Dataset
                                 Column)))

(defn columns
  "Returns all column names as an array."
  ^"[Ljava.lang.String;" [^Dataset ds]
  (. ds columns))

(defn ->df
  ^Dataset [^Dataset ds xs]
  (. ds toDF ^"[Ljava.lang.String;" (into-array String xs)))

(defn ->java-rdd
  "Returns the content of the Dataset as a JavaRDD of Ts."
  ^JavaRDD [^Dataset ds]
  (. ds toJavaRDD))

(defn filter
  "Filters rows using the given condition."
  ^Dataset [^Dataset ds ^Column column]
  (. ds filter column))
