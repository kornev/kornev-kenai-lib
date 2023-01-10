(ns kenai.rdd.rdd
  (:refer-clojure :exclude [map mapcat partition-by])
  (:require [clojure.string :as str]
            [clojure.set :refer (map-invert)]
            [kenai.spark.storage :as api-storage]
            [kenai.spark.rdd.java-rdd-like :as api-java-rdd-like]
            [kenai.spark.function :as api-function])
  (:import (com.google.common.collect Ordering)
           (org.apache.spark HashPartitioner
                             Partitioner)
           (kenai.spark ClojureHashPartitioner)
           (org.apache.spark.api.java JavaRDD
                                      JavaPairRDD
                                      JavaRDDLike)))

(defn storage-level
  "Get the RDD's current storage level, or StorageLevel.NONE if none is set."
  [^JavaRDD rdd]
  ((map-invert api-storage/storage-levels)
   (. rdd getStorageLevel)))

(defn cache!
  "Set this RDD's storage level to persist its values across operations after the first time it is computed."
  (^JavaRDD [^JavaRDD rdd]
   (cache! rdd :memory-only))
  (^JavaRDD [^JavaRDD rdd level]
   {:pre [(contains? api-storage/storage-levels level)]}
   (->> (api-storage/storage-levels level)
        (. rdd persist))))

(defn uncache!
  "Mark the RDD as non-persistent, and remove all blocks for it from memory and disk."
  (^JavaRDD [^JavaRDD rdd]
   (. rdd unpersist true))
  (^JavaRDD [^JavaRDD rdd blocking?]
   (->> (boolean blocking?)
        (. rdd unpersist))))

(defn checkpointed?
  "Return whether this RDD is checkpointed and materialized, either reliably or locally."
  [^JavaRDDLike rdd]
  (. rdd isCheckpointed))

(defn checkpoint!
  "Mark this RDD for checkpointing."
  [^JavaRDDLike rdd]
  (. rdd checkpoint))

(defn- ^:no-doc fn-name
  "Return the (unmangled) name of the given Clojure function."
  [f]
  (Compiler/demunge (.getName (class f))))

(defn- internal-call?
  "True if a stack-trace element should be ignored because it represents an internal
  function call that should not be considered a callsite."
  [^StackTraceElement element]
  (let [class-name (.getClassName element)]
    (or (str/starts-with? class-name "kenai.")
        (str/starts-with? class-name "clojure.lang."))))

(defn- stack-callsite
  "Find the top element in the current stack trace that is not an internal
  function call."
  ^StackTraceElement
  []
  (first (remove internal-call? (.getStackTrace (Exception.)))))

(defn- callsite-name
  "Generate a name for the callsite of this function by looking at the current
  stack. Ignores core Clojure and internal function frames."
  []
  (let [callsite (stack-callsite)
        filename (.getFileName callsite)
        classname (.getClassName callsite)
        line-number (.getLineNumber callsite)]
    (format "%s %s:%d" (Compiler/demunge classname) filename line-number)))

(defn ^:no-doc set-callsite-name
  "Provide a name for the given RDD by looking at the current stack. Returns
  the updated RDD if the name could be determined."
  ^JavaRDD [^JavaRDD rdd & args]
  (try
    (let [rdd-name (format "#<%s: %s %s>"
                           (.getSimpleName (class rdd))
                           (callsite-name)
                           (if (seq args)
                             (str " [" (str/join ", " args) "]")
                             ""))]
      (.setName rdd rdd-name))
    (catch Exception _
      ;; Ignore errors and return an unnamed RDD.
      rdd)))

(defn num-partitions [^JavaRDDLike rdd]
  "Returns the number of partitions of this RDD."
  (. rdd getNumPartitions))

(defn coalesce
  "Return a new RDD that is reduced into numPartitions partitions."
  (^JavaRDD [^JavaRDD rdd]
   (coalesce rdd num-partitions false))
  (^JavaRDD [^JavaRDD rdd num-partitions shuffle?]
   (-> (. rdd coalesce (int num-partitions) (boolean shuffle?))
       (set-callsite-name num-partitions shuffle?))))

(defn repartition
  "Return a new RDD that has exactly numPartitions partitions."
  ^JavaRDD [^JavaRDD rdd num-partitions]
  (-> (. rdd repartition (int num-partitions))
      (set-callsite-name num-partitions)))

(defn repartition-and-sort-within-partitions
  "Repartition the RDD according to the given partitioner and, within each resulting partition,
  sort records by their keys."
  (^JavaPairRDD [^JavaPairRDD rdd ^Partitioner partitioner]
   (repartition-and-sort-within-partitions rdd partitioner (Ordering/natural)))
  (^JavaPairRDD [^JavaPairRDD rdd ^Partitioner partitioner ^java.util.Comparator comparator]
   (-> (. rdd repartitionAndSortWithinPartitions partitioner comparator)
       (set-callsite-name partitioner comparator))))

(defn partition-by
  "Return a copy of `rdd` partitioned by the given `partitioner`."
  ^JavaPairRDD [^JavaPairRDD rdd ^Partitioner partitioner]
  (-> (. rdd partitionBy partitioner)
      (set-callsite-name partitioner)))

(defn hash-partitioner
  "Construct a partitioner which will hash keys to distribute them uniformly
  over `n` buckets. Optionally accepts a `key-fn` which will be called on each
  key before hashing it."
  (^Partitioner [n]
   (HashPartitioner. (int n)))
  (^Partitioner [key-fn n]
   (ClojureHashPartitioner. (int n) (api-function/fn1 key-fn))))

(defn map
  "Map the function `f` over each element of `rdd`. Returns a new RDD
  representing the transformed elements."
  ^JavaRDDLike [^JavaRDDLike rdd f]
  (-> (api-java-rdd-like/map rdd f)
      (set-callsite-name (fn-name f))))

(defn mapcat
  "Map the function `f` over each element in `rdd` to produce a sequence of
  results. Returns an RDD representing the concatenation of all element
  results."
  ^JavaRDD [^JavaRDDLike rdd f]
  (-> (api-java-rdd-like/flat-map rdd f)
      (set-callsite-name (fn-name f))))
