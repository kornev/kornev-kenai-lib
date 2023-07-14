(ns kenai.lifecycle
  (:require [clojure.string :as str]
            [kenai.spark.spark-context :as api-spark-context]
            [kenai.spark.sql.spark-session :as api-spark-session])
  (:import (org.apache.spark SparkConf)
           (org.apache.spark.sql SparkSession)))

(defn- spark-conf
  "Construct a new Spark configuration."
  ^SparkConf [{:keys [app-name settings]
               :or   {app-name "kenai-app"
                      settings {}}}]
  (reduce (fn [^SparkConf state [k v]] (. state set (name k) (str v)))
          (-> (SparkConf. false)
              (. set "spark.app.name" app-name)
              (. set "spark.ui.enabled" "false"))
          settings))

(defn get-or-create
  "The entry point to programming Spark with the Dataset and DataFrame API."
  ^SparkSession [{:as params
                  :keys [checkpoint-dir]}]
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
  [^SparkSession spark level]  ; ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN
  (api-spark-context/set-log-level
   (-> spark
       api-spark-session/spark-context
       api-spark-context/java-spark-context)
   (-> level
       name
       str/upper-case)))

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
