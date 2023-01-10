(ns kenai.sql.dataset
  (:refer-clojure :exclude [filter])
  (:require [kenai.spark.sql.data-types :as api-types]
            [kenai.spark.sql.dataset :as api-dataset]
            [kenai.spark.sql.spark-session :as api-spark-session]
            [kenai.sql.column :as column])
  (:import (org.apache.spark.sql Column
                                 Dataset
                                 SparkSession)
           (org.apache.spark.sql.types StructType)))

(defn schema
  "Coerces plain Clojure data structures to a Spark schema."
  ^StructType [value]
  (cond
    (and (vector? value) (= 1 (count value)))
    (api-types/create-array-type (schema (first value)))

    (and (vector? value) (= 2 (count value)))
    (api-types/create-map-type (schema (first value))
                               (schema (second value)))

    (map? value)
    (->> value
         (map (fn [[k v]] (api-types/create-struct-field k (schema v))))
         (apply api-types/create-struct-type))

    :else
    value))

(defn dataframe
  (^Dataset [^SparkSession spark]
   (api-spark-session/empty-dataframe spark))
  (^Dataset [^SparkSession spark rows schema]
   (api-spark-session/create-dataframe spark rows schema)))

;;; Transformations

(defn filter
  ^Dataset [^Dataset df expr]
  (api-dataset/filter df (. ^Column (column/col expr) cast "boolean")))

;;; Actions

(defn show
  ([^Dataset df]
   (show df {}))
  ([^Dataset df opts]
   (let [{:keys [num-rows truncate vertical]
          :or   {num-rows 20
                 truncate 0
                 vertical false}} opts]
     (-> (. df showString num-rows truncate vertical) println))))

(defn show-vertical
  "Displays the Dataset in a list-of-records form."
  ([^Dataset df]
   (show df {:vertical true}))
  ([^Dataset df options]
   (show df (assoc options :vertical true))))

(defn print-schema
  [^Dataset df]
  (-> df .schema .treeString println))
