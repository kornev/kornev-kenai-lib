(ns kenai.spark.sql.spark-session
  (:refer-clojure :exclude [range])
  (:import (org.apache.spark SparkContext)
           (org.apache.spark.api.java JavaRDD)
           (org.apache.spark.sql Dataset
                                 SparkSession)
           (org.apache.spark.sql.catalog Catalog)
           (org.apache.spark.sql.types StructType)))

(defn spark-context
  ^SparkContext [^SparkSession spark]
  (. spark sparkContext))

(defn catalog
  "Interface through which the user may create, drop, alter or query underlying databases, tables, functions etc."
  ^Catalog [^SparkSession spark]
  (. spark catalog))

(defn sql
  "Executes a SQL query using Spark, returning the result as a DataFrame.
  The dialect that is used for SQL parsing can be configured with 'spark.sql.dialect'."
  ^Dataset [^SparkSession spark ^String sql-text]
  (. spark sql sql-text))

(defn empty-dataframe
  "Returns a `DataFrame` with no rows or columns."
  ^Dataset [^SparkSession spark]
  (. spark emptyDataFrame))

(defmulti create-dataframe
  "Creates a DataFrame from a (JavaRDD || java.util.List) containing Rows using the given schema."
  (fn [& args] (mapv class args)))
(defmethod create-dataframe [SparkSession java.util.List StructType]
  [^SparkSession spark ^java.util.List rows ^StructType schema]
  (. spark createDataFrame rows schema))
(defmethod create-dataframe [SparkSession JavaRDD StructType]
  [^SparkSession spark ^JavaRDD rdd ^StructType schema]
  (. spark createDataFrame rdd schema))

(defmulti range
  "Creates a Dataset with a single LongType column named id."
  (fn [& args] (mapv class args)))
(defmethod range [SparkSession Long]
  [^SparkSession spark ^Long end]
  (. spark range end))
(defmethod range [SparkSession Long Long]
  [^SparkSession spark ^Long start ^Long end]
  (. spark range start end))
(defmethod range [SparkSession Long Long Long]
  [^SparkSession spark ^Long start ^Long end ^Long step]
  (. spark range start end step))
(defmethod range [SparkSession Long Long Long Long]
  [^SparkSession spark ^Long start ^Long end ^Long step ^Integer num-partitions]
  (. spark range start end step num-partitions))
