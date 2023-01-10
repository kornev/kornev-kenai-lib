(ns kenai.spark.sql.data-frame-writer
  (:refer-clojure :exclude [partition-by])
  (:import (org.apache.spark.sql DataFrameWriter)))

(defn mode
  "Specifies the behavior when data or table already exists."
  ^DataFrameWriter [^DataFrameWriter writer save-mode]
  (. writer mode (name save-mode)))

(defn partition-by
  "Partitions the output by the given columns on the file system."
  ^DataFrameWriter [^DataFrameWriter writer xs]
  (. writer partitionBy ^"[Ljava.lang.String;" (into-array String xs)))

(defn options
  "Adds input options for the underlying data source."
  ^DataFrameWriter [^DataFrameWriter writer ^java.util.Map m]
  (. writer options m))
