(ns kenai.lifecycle
  (:require [kenai.spark.spark-context :as api-spark-context]
            [kenai.spark.sql.spark-session :as api-spark-session])
  (:import (org.apache.spark SparkConf)
           (org.apache.spark.sql SparkSession)))

(defn- spark-conf
  "Construct a new Spark configuration."
  ^SparkConf [{:keys [app-name master settings]
               :or   {app-name "kenai-app"
                      master   "local[2]"
                      settings {}}}]
  (reduce (fn [^SparkConf state [k ^String v]]
            (. state set (name k) v))
          (.. (SparkConf. false)
              (setAppName app-name)
              (setMaster master))
          (merge {"spark.ui.enabled" "false"}
                 settings)))

(defn get-or-create
  "The entry point to programming Spark with the Dataset and DataFrame API."
  ^SparkSession [{:keys [checkpoint-dir] :as params}]
  (let [settings (spark-conf params)
        session  (.. (SparkSession/builder)
                     (config settings)
                     (enableHiveSupport)
                     (getOrCreate))
        context  (api-spark-session/spark-context session)]
    (when checkpoint-dir
      (. context setCheckpointDir checkpoint-dir))
    session))

(defn log-level!
  "Control our logLevel."
  [^SparkSession spark level]
  (-> (api-spark-session/spark-context spark)
      (api-spark-context/java-spark-context)
      (api-spark-context/set-log-level level)))

(defn settings
  "Return a copy of this JavaSparkContext's configuration. The configuration \"cannot\" be changed at runtime."
  [^SparkSession spark]
  (-> (api-spark-session/spark-context spark)
      (api-spark-context/java-spark-context)
      (api-spark-context/get-conf)))

(defn summary
  [^SparkSession spark]
  (let [sc (api-spark-session/spark-context spark)
        jc (api-spark-context/java-spark-context sc)]
    {:app-name               (. jc appName)
     :app-id                 (api-spark-context/application-id sc)
     :master                 (. jc master)
     :local?                 (. jc isLocal)
     :user                   (. jc sparkUser)
     :start-time             (. jc startTime)
     :jars                   (api-spark-context/jars jc)
     :default-min-partitions (. jc defaultMinPartitions)
     :default-parallelism    (. jc defaultParallelism)
     :checkpoint-dir         (api-spark-context/get-checkpoint-dir jc)
     :version                (. jc version)}))
