(ns kenai.spark.rdd.java-rdd-like
  (:refer-clojure :exclude [map])
  (:require [kenai.spark.function :as function])
  (:import (org.apache.spark.api.java JavaRDD
                                      JavaRDDLike)))

(defn map
  "Return a new RDD by applying a function to all elements of this RDD."
  ^JavaRDD [^JavaRDDLike rdd f]
  (. rdd map (function/fn1 f)))

(defn flat-map
  "Return a new RDD by first applying a function to all elements of this
  RDD, and then flattening the results."
  ^JavaRDD [^JavaRDDLike rdd f]
  (. rdd flatMap (function/flat-map-fn1 f)))
