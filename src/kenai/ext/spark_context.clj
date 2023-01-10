(ns kenai.ext.spark-context)

;(defn set-checkpoint-dir!
;  "Set the directory under which RDDs are going to be checkpointed."
;  [^JavaSparkContext spark-context path]
;  (.setCheckpointDir spark-context path))

;(defn app-name
;  ([spark] (-> spark java-spark-context .appName)))

;(defmethod binary-files SparkSession
;  ([spark path] (.binaryFiles (java-spark-context spark) path))
;  ([spark path num-partitions]
;   (.binaryFiles (java-spark-context spark) path num-partitions)))

;(defn broadcast
;  ([spark value] (-> spark java-spark-context (.broadcast value))))

;(defn default-min-partitions
;  ([spark] (-> spark java-spark-context .defaultMinPartitions)))

;(defn default-parallelism
;  ([spark] (-> spark java-spark-context .defaultParallelism)))

;(defn empty-rdd
;  ([spark] (-> spark java-spark-context .emptyRDD)))
;
;(defn is-local
;  ([spark] (-> spark java-spark-context .isLocal)))

;(defn get-local-property
;  ([spark k] (-> spark java-spark-context (.getLocalProperty k))))

;(defn master
;  ([spark] (-> spark java-spark-context .master)))

;;; TODO: support min-partitions arg
;(defn parallelize
;  ([spark data] (-> spark
;                    java-spark-context
;                    (.parallelize data)
;                    unmangle/unmangle-name)))

;(defn parallelize-doubles
;  ([spark data]
;   (-> spark
;       java-spark-context
;       (.parallelizeDoubles (map double data))
;       unmangle/unmangle-name)))

;(defn parallelize-pairs
;  ([spark data]
;   (-> spark
;       java-spark-context
;       (.parallelizePairs (map interop/->scala-tuple2 data))
;       unmangle/unmangle-name)))

;(defn get-persistent-rd-ds
;  ([spark] (->> spark java-spark-context .getPersistentRDDs (into {}))))

;(defn resources
;  ([spark] (->> spark java-spark-context .resources (into {}))))

;(defn get-spark-home
;  ([spark] (-> spark java-spark-context .getSparkHome interop/optional->nillable)))

;(defmethod text-file SparkSession
;  ([spark path] (-> spark java-spark-context (.textFile path)))
;  ([spark path min-partitions] (-> spark java-spark-context (.textFile path min-partitions))))

;(defn version
;  ([spark] (-> spark java-spark-context .version)))

;(defmethod whole-text-files SparkSession
;  ([spark path]
;   (.wholeTextFiles (java-spark-context spark) path))
;  ([spark path min-partitions]
;   (.wholeTextFiles (java-spark-context spark) path min-partitions)))

;(defn set-job-description!
;  "Set a human readable description of the current job."
;  [^JavaSparkContext spark-context description]
;  (.setJobDescription spark-context description))

;(defn set-job-group!
;  "Assign a group ID to all the jobs started by this thread until the group ID
;  is set to a different value or cleared.
;
;  Often, a unit of execution in an application consists of multiple Spark
;  actions or jobs. Application programmers can use this method to group all
;  those jobs together and give a group description. Once set, the Spark web UI
;  will associate such jobs with this group.
;
;  The application can later use `cancel-job-group!` to cancel all running jobs
;  in this group. If `interrupt?` is set to true for the job group, then job
;  cancellation will result in the job's executor threads being interrupted."
;  ([^JavaSparkContext spark-context group-id description]
;   (.setJobGroup spark-context group-id description))
;  ([^JavaSparkContext spark-context group-id description interrupt?]
;   (.setJobGroup spark-context group-id description (boolean interrupt?))))

;(defn clear-job-group!
;  "Clear the current thread's job group ID and its description."
;  [^JavaSparkContext spark-context]
;  (.clearJobGroup spark-context))

;(defn cancel-job-group!
;"Cancel active jobs for the specified group.
;
;See `set-job-group!` for more information."
;[^JavaSparkContext spark-context group-id]
;(.cancelJobGroup spark-context group-id))

;(defn cancel-all-jobs!
;"Cancel all jobs that have been scheduled or are running."
;[^JavaSparkContext spark-context]
;(.cancelAllJobs spark-context))

;(defn get-local-property
;  "Get a local property set for this thread, or null if not set."
;  [^JavaSparkContext spark-context k]
;  (.getLocalProperty spark-context k))

;(defn persistent-rdds
;  "Return a Java map of JavaRDDs that have marked themselves as persistent via
;  a `cache!` call."
;  [^JavaSparkContext spark-context]
;  (into {} (.getPersistentRDDs spark-context)))

;(defn add-file!
;  "Add a file to be downloaded with this Spark job on every node."
;  ([^JavaSparkContext spark-context path]
;   (.addFile spark-context path))
;  ([^JavaSparkContext spark-context path recursive?]
;   (.addFile spark-context path (boolean recursive?))))

;(defn add-jar!
;  "Adds a JAR dependency for all tasks to be executed on this SparkContext in
;  the future."
;  [^JavaSparkContext spark-context path]
;  (.addJar spark-context path))

;(defn set-local-property!
;  "Set a local property that affects jobs submitted from this thread, and all
;  child threads, such as the Spark fair scheduler pool."
;  [^JavaSparkContext spark-context k v]
;  (.setLocalProperty spark-context k v))

;(defn set-log-level!
;  "Control the Spark application's logging level."
;  [^JavaSparkContext spark-context level]
;  (.setLogLevel spark-context level))
