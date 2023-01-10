(ns kenai.sql.dataset-creation
  (:require [kenai.spark.sql.row-factory :as api-row-factory]
            [kenai.spark.sql.spark-session :as api-spark-session])
  (:import (org.apache.spark.sql.types ArrayType
                                       DataTypes)
           (org.apache.spark.sql SparkSession)
           (org.apache.spark.sql.types StructType)))

(def java-type->spark-type
  "A mapping from Java types to Spark types."
  {java.lang.String   DataTypes/StringType
   java.lang.Boolean  DataTypes/BooleanType
   java.util.Date     DataTypes/DateType
   java.sql.Timestamp DataTypes/TimestampType
   java.lang.Double   DataTypes/DoubleType
   java.lang.Float    DataTypes/FloatType
   java.lang.Byte     DataTypes/ByteType
   java.lang.Integer  DataTypes/IntegerType
   java.lang.Long     DataTypes/LongType
   java.lang.Short    DataTypes/ShortType
   nil                DataTypes/NullType})

(declare infer-schema infer-spark-type)

(defn- infer-spark-type
  [value]
  (cond
    (map? value)
    (infer-schema (map name (keys value)) (vals value))

    (coll? value)
    (ArrayType. (infer-spark-type (first value)) true)

    :else
    (get java-type->spark-type (type value) DataTypes/BinaryType)))

(defn- infer-struct-field
  [col-name value]
  (let [spark-type (infer-spark-type value)]
    (DataTypes/createStructField col-name spark-type true)))

(defn- infer-schema
  [col-names values]
  (DataTypes/createStructType ^java.util.List (mapv infer-struct-field col-names values)))

(defn- update-val-in
  "Works similar to update-in but accepts value instead of function.
  If old and new values are collections, merges/concatenates them.
  If the associative structure is nil, initialises it to provided value."
  [m path val]
  (let [f (fn [old new]
            (cond (nil? old)                                  new
                  (and (map? old) (map? new))                 (merge old new)
                  (and (coll? new) (= (type old) (type new))) (into old new)
                  :else                                       new))]
    (if-not m val (update-in m path f val))))

(defn- first-non-nil
  "Looks through values and recursively finds the first non-nil value.
  For maps, it returns a first non-nil value for each nested key.
  For list of maps, it returns a list of one map with first non-nil value for each nested key.

    Examples:
    []                                          => []
    [nil nil]                                   => []
    [1 2 3]                                     => [1]
    [nil [1 2]]                                 => [[1]]
    [{:a 1} {:a 3 :b true}]                     => [{:a 1 :b true}]
    [{:a 1} {:b [{:a 4} {:c 3}]}]               => [{:a 1 :b [{:a 4 :c 3}]}]
    [{:a 1} {:b [[{:a 4} {:c 3}] [{:h true}]]}] => [{:a 1 :b [[{:a 4 :c 3 :h true}]]}]"
  ([v]
   (first-non-nil v nil []))
  ([v non-nil path]
   (cond (map? v)
         (reduce #(first-non-nil (get v %2) %1 (conj path %2)) (update-val-in non-nil path {}) (keys v))

         (coll? v)
         (reduce (fn [non-nil v]
                   (let [path (conj path 0)
                         non-nil (first-non-nil v non-nil path)]
                     (if (coll? (get-in non-nil path)) non-nil (reduced non-nil))))
                 (update-val-in non-nil path [])
                 (filter (complement nil?) v))

         (or (nil? v) (some? (get-in non-nil path)))
         non-nil

         :else
         (update-val-in non-nil path v))))

(defn- fill-missing-nested-keys
  "Recursively fills in any missing keys. Takes as input the records and a sample non-nil value.
  The sample non-nil value can be generated using first-non-nil function above.

    Examples:
    [] | []
     => []
    [nil nil] | []
     => [nil nil]
    [1 2 3] | [1]
     => [1 2 3]
    [nil [1 2]] | [[1]]
     => [nil [1 2]]
    [{:a 1} {:a 3 :b true}] | [{:a 1 :b true}]
     => [{:a 1 :b nil} {:a 3 :b true}]
    [{:a 1} {:b [{:a 4} {:c 3}]}] | [{:a 1 :b [{:a 4 :c 3}]}])
     => [{:a 1 :b nil} {:a nil :b [{:a 4 :c nil} {:a nil :c 3}]}]
    [{:a 1} {:b [[{:a 4} {:c 3}] [{:h true}]]}] | [{:a 1 :b [[{:a 4 :c 3 :h true}]]}]
     => [{:a 1 :b nil} {:a nil :b [[{:a 4 :c nil :h nil} {:a nil :c 3 :h nil}] [{:a nil :c nil :h true}]]}]"
  ([v non-nil]
   (fill-missing-nested-keys v non-nil []))
  ([v non-nil path]
   (cond (map? v)
         (reduce #(assoc %1 %2 (fill-missing-nested-keys (get v %2) non-nil (conj path %2)))
                 {}
                 (keys (get-in non-nil path)))

         (and (coll? v)
              (coll? (get-in non-nil (conj path 0))))
         (map #(fill-missing-nested-keys % non-nil (conj path 0)) v)

         :else
         v)))

(defn- transpose [xs]
  (apply map list xs))

(defn- transform-maps
  [value]
  (cond (map? value)  (api-row-factory/create (transform-maps (vals value)))
        (coll? value) (map transform-maps value)
        :else        value))

(defn table->dataset
  "Construct a Dataset from a collection of collections.

  ```clojure
  (k/show (k/table->dataset [[1 2] [3 4]] [:a :b]))
  ; +---+---+
  ; |a  |b  |
  ; +---+---+
  ; |1  |2  |
  ; |3  |4  |
  ; +---+---+
  ```"
  [^SparkSession spark table col-names]
  (if (empty? table)
    (.emptyDataFrame spark)
    (let [col-names  (map name col-names)
          transposed (transpose table)
          values     (map first-non-nil transposed)
          table      (transpose (map (partial apply fill-missing-nested-keys)
                                     (map vector transposed values)))
          rows       (map api-row-factory/create (transform-maps table))
          schema     (infer-schema col-names (map first values))]
      (api-spark-session/create-dataframe spark rows schema))))

(defn map->dataset
  "Construct a Dataset from an associative map.

  ```clojure
  (k/show (k/map->dataset {:a [1 2] :b [3 4]}))
  ; +---+---+
  ; |a  |b  |
  ; +---+---+
  ; |1  |3  |
  ; |2  |4  |
  ; +---+---+
  ```"
  [^SparkSession spark map-of-values]
  (if (empty? map-of-values)
    (.emptyDataFrame spark)
    (let [table     (transpose (vals map-of-values))
          col-names (keys map-of-values)]
      (table->dataset spark table col-names))))

(defn- conj-record
  [map-of-values record]
  (let [col-names (keys map-of-values)]
    (reduce (fn [acc-map col-name]
              (update acc-map col-name #(conj % (get record col-name))))
            map-of-values
            col-names)))

(defn records->dataset
  "Construct a Dataset from a collection of maps.

  ```clojure
  (k/show (k/records->dataset [{:a 1 :b 2} {:a 3 :b 4}]))
  ; +---+---+
  ; |a  |b  |
  ; +---+---+
  ; |1  |2  |
  ; |3  |4  |
  ; +---+---+
  ```"
  [spark records]
  (let [col-names     (-> (map keys records) flatten distinct)
        map-of-values (reduce conj-record
                              (zipmap col-names (repeat []))
                              records)]
    (map->dataset spark map-of-values)))
