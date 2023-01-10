(ns kenai.spark.sql.data-frame-reader
  (:import (org.apache.spark.sql DataFrameReader)
           (org.apache.spark.sql.types StructType)))

(defn options
  "Adds input options for the underlying data source."
  ^DataFrameReader [^DataFrameReader reader ^java.util.Map m]
  (. reader options m))

(defn schema
  "Specifies the schema by using the input DDL-formatted string."
  ^DataFrameReader [^DataFrameReader reader ^StructType schema]
  (. reader schema schema))
