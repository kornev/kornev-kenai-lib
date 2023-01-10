(ns kenai.sql.column
  (:import (clojure.lang Keyword)
           (org.apache.spark.sql Column
                                 Dataset
                                 functions)))

(defn lit
  "Creates a Column of literal value."
  ^Column [arg1]
  (let [normed-arg (if (coll? arg1)
                     (-> (first arg1) (type) (into-array arg1))
                     arg1)]
    (functions/lit normed-arg)))

(defmulti col
  (fn [head & _] (class head)))
(defmethod col Keyword
  [x & _]
  (functions/col (name x)))
(defmethod col String
  [x & _]
  (functions/col x))
(defmethod col Column
  [x & _]
  x)
(defmethod col Dataset
  [^Dataset dataset & args]
  (. dataset col (name (first args))))
(defmethod col :default
  [x & _]
  (lit x))

(defmulti as
  (fn [head & more] [(class head) (if (> (count more) 1) true false)]))
(defmethod as [Column true]
  [^Column column & new-names]
  (. column as ^"[Ljava.lang.String;" (into-array String (map name new-names))))
(defmethod as [Dataset false]
  [^Dataset dataset new-name]
  (. dataset as (name new-name)))
(defmethod as :default
  [expr new-name]
  (. ^Column (col expr) as (name new-name)))
