;; Docstring Sources:
;; https://numpy.org/doc/
;; https://pandas.pydata.org/docs/
(ns kenai.ext.foreign-idioms
  (:refer-clojure :exclude [replace])
  (:require [clojure.string :as string]
            [kenai.ext.dataset :as dataset]
            [kenai.sql.column :as column]
            [kenai.ext.column :as ext-column]
            [kenai.conversions :as conversions]
            [kenai.ext.polymorphic :as polymorphic]
            [kenai.ext.functions :as sql]
            [kenai.sql.data-sources :as data-sources]
            [kenai.utils :refer (link)])
  (:import
   (org.apache.spark.sql Column functions)))

;; Start patch
(link column/col ->column)
(link conversions/lift-coll ensure-coll)
;; End patch

;; NumPy
(defn clip
  "Returns a new Column where values outside `[low, high]` are clipped to the interval edges."
  [expr low high]
  (let [col (->column expr)]
    (-> (polymorphic/coalesce
         (sql/when (ext-column/<= col low) low)
         (sql/when (ext-column/<= high col) high)
         col)
        (column/as (format "clip(%s, %s, %s)"
                           (.toString col)
                           (str low)
                           (str high))))))

(defn random-uniform
  "Returns a new Column of draws from a uniform distribution."
  ([] (random-uniform 0.0 1.0))
  ([low high] (random-uniform low high (rand-int Integer/MAX_VALUE)))
  ([low high seed]
   (let [length (Math/abs (- high low))
         base   (min high low)]
     (ext-column/+ base (ext-column/* length (sql/rand seed))))))
(link random-uniform runiform)
(link random-uniform runif)

(defn random-norm
  "Returns a new Column of draws from a normal distribution."
  ([] (random-norm 0.0 1.0))
  ([mu sigma] (random-norm mu sigma (rand-int Integer/MAX_VALUE)))
  ([mu sigma seed] (ext-column/+ mu (ext-column/* sigma (sql/randn seed)))))
(link random-norm rnorm)

(defn random-exp
  "Returns a new Column of draws from an exponential distribution."
  ([] (random-exp 1.0))
  ([rate] (random-exp rate (rand-int Integer/MAX_VALUE)))
  ([rate seed] (-> (sql/rand seed)
                   sql/log
                   (ext-column/* -1.0)
                   (ext-column// rate))))
(link random-exp rexp)

(defn random-int
  "Returns a new Column of random integers from `low` (inclusive) to `high` (exclusive)."
  ([] (random-int 0 (dec Integer/MAX_VALUE)))
  ([low high] (random-int low high (rand-int Integer/MAX_VALUE)))
  ([low high seed]
   (let [length (Math/abs (- high low))
         base   (min high low)
         ->long #(ext-column/cast % "long")]
     (ext-column/+ (->long base) (->long (ext-column/* length (sql/rand seed)))))))

(defn random-choice
  "Returns a new Column of a random sample from a given collection of `choices`."
  ([choices]
   (let [n-choices (count choices)]
     (random-choice choices (take n-choices (repeat (/ 1.0 n-choices))))))
  ([choices probs] (random-choice choices probs (rand-int Integer/MAX_VALUE)))
  ([choices probs seed]
   (assert (and (= (count choices) (count probs))
                (every? pos? probs))
           "random-choice args must have same lengths.")
   (assert (< (Math/abs (- (apply + probs) 1.0)) 1e-4)
           "random-choice probs must some to one.")
   (let [rand-col    (->column (sql/rand seed))
         cum-probs   (reductions + probs)
         choice-cols (map (fn [choice prob]
                            (sql/when (ext-column/< rand-col (+ prob 1e-6))
                              (->column choice)))
                          choices
                          cum-probs)]
     (.as (apply polymorphic/coalesce choice-cols)
          (format "choice(%s, %s)" (str choices) (str probs))))))
(link random-choice rchoice)

;; Pandas
(defn value-counts
  "Returns a Dataset containing counts of unique rows.

  The resulting object will be in descending order so that the
  first element is the most frequently-occurring element."
  [dataframe]
  (-> dataframe
      (dataset/group-by (dataset/columns dataframe))
      (dataset/agg {:count (functions/count "*")})
      (dataset/order-by (.desc (->column :count)))))

(defn shape
  "Returns a vector representing the dimensionality of the Dataset."
  [dataframe]
  [(.count dataframe) (count (.columns dataframe))])

(defn nlargest
  "Return the Dataset with the first `n-rows` rows ordered by `expr` in descending order."
  [dataframe n-rows expr]
  (-> dataframe
      (dataset/order-by (.desc (->column expr)))
      (dataset/limit n-rows)))

(defn nsmallest
  "Return the Dataset with the first `n-rows` rows ordered by `expr` in ascending order."
  [dataframe n-rows expr]
  (-> dataframe
      (dataset/order-by (->column expr))
      (dataset/limit n-rows)))

(defn nunique
  "Count distinct observations over all columns in the Dataset."
  [dataframe]
  (dataset/agg-all dataframe #(functions/countDistinct
                               (->column %)
                               (into-array Column []))))

(defn- resolve-probs [num-buckets-or-probs]
  (if (coll? num-buckets-or-probs)
    (do
      (assert (and (apply < num-buckets-or-probs)
                   (every? #(< 0.0 % 1.0) num-buckets-or-probs))
              "Probs array must be increasing and in the unit interval.")
      num-buckets-or-probs)
    (map #(/ (inc %) (double num-buckets-or-probs)) (range (dec num-buckets-or-probs)))))

;; (defn qcut
;;   "Returns a new Column of discretised `expr` into equal-sized buckets based
;;   on rank or based on sample quantiles."
;;   [expr num-buckets-or-probs]
;;   (let [probs     (resolve-probs num-buckets-or-probs)
;;         col       (column/->column expr)
;;         rank-col  (window/windowed {:window-col (sql/percent-rank) :order-by col})
;;         qcut-cols (map (fn [low high]
;;                          (sql/when (column/<= low rank-col high)
;;                            (column/lit (format "%s[%s, %s]"
;;                                                (.toString col)
;;                                                (str low)
;;                                                (str high)))))
;;                        (concat [0.0] probs)
;;                        (concat probs [1.0]))]
;;     (.as (apply polymorphic/coalesce qcut-cols)
;;          (format "qcut(%s, %s)" (.toString col) (str probs)))))

(defn cut
  "Returns a new Column of discretised `expr` into the intervals of bins."
  [expr bins]
  (assert (apply < bins))
  (let [col      (->column expr)
        cut-cols (map (fn [low high]
                        (sql/when (ext-column/<= low col high)
                          (column/lit (format "%s[%s, %s]"
                                              (.toString col)
                                              (str low)
                                              (str high)))))
                      (concat [Double/NEGATIVE_INFINITY] bins)
                      (concat bins [Double/POSITIVE_INFINITY]))]
    (.as (apply polymorphic/coalesce cut-cols)
         (format "cut(%s, %s)" (.toString col) (str bins)))))

(defn replace
  "Returns a new Column where `from-value-or-values` is replaced with `to-value`."
  ([expr lookup-map]
   (reduce-kv (fn [column from to] (replace column from to)) expr lookup-map))
  ([expr from-value-or-values to-value]
   (let [from-values (ensure-coll from-value-or-values)]
     (sql/when
      (ext-column/isin expr from-values)
       (column/lit to-value)
       expr))))

;; Tech ML
(defn- apply-options [dataset options]
  (-> dataset
      (cond-> (:column-whitelist options)
        (dataset/select (map name (:column-whitelist options))))
      (cond-> (:n-records options)
        (dataset/limit (:n-records options)))))

;(defmulti ->dataset
;          "Create a Dataset from a path or a collection of records."
;          (fn [head & _] (class head)))
;
; TODO: support excel files
;(defmethod ->dataset java.lang.String
;  ([path]
;   (cond
;     (string/includes? path ".avro") (data-sources/read-avro! path)
;     (string/includes? path ".csv") (data-sources/read-csv! path)
;     (string/includes? path ".json") (data-sources/read-json! path)
;     (string/includes? path ".parquet") (data-sources/read-parquet! path)
;     :else (throw (Exception. "Unsupported file format."))))
;  ([path options] (apply-options (->dataset path) options)))

;; (defmethod ->dataset :default
;;   ([records] (dataset-creation/records->dataset records))
;;   ([records options] (apply-options (->dataset records) options)))

;; (import-fn dataset-creation/map->dataset name-value-seq->dataset)

(link dataset/select select-columns)
