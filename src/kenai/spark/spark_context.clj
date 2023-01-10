(ns kenai.spark.spark-context
  (:require [clojure.walk :as walk]
            [kenai.scala.tuple :refer (->clj-vec)
                               :rename {->clj-vec tuple-scala->clj}])
  (:import (org.apache.spark SparkContext)
           (org.apache.spark.api.java JavaSparkContext)))

(defn java-spark-context
  "Converts a SparkSession to a JavaSparkContext."
  ^JavaSparkContext [^SparkContext ctx]
  (JavaSparkContext/fromSparkContext ctx))

(defn get-conf
  "Return a copy of this JavaSparkContext's configuration."
  [^JavaSparkContext ctx]
  (->> (. ctx getConf)
       (.getAll)
       (map tuple-scala->clj)
       (into {})
       (walk/keywordize-keys)))

(defn application-id
  "A unique identifier for the Spark application."
  [^SparkContext ctx]
  (. ctx applicationId))

(defn jars
  [^JavaSparkContext ctx]
  (vec (. ctx jars)))

(defn get-checkpoint-dir
  ([^JavaSparkContext ctx]
   (. ^org.apache.spark.api.java.Optional (. ctx getCheckpointDir) orNull)))

(defn set-log-level
  "Control our logLevel. This overrides any user-defined log settings."
  [^JavaSparkContext ctx level]
  (. ctx setLogLevel (name level)))
