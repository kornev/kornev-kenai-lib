(ns kenai.ext.column
  (:refer-clojure :exclude [-
                            +
                            *
                            /
                            <
                            <=
                            >
                            >=
                            mod
                            cast])
  (:require [kenai.sql.column :as column]
            [kenai.scala.collection :as scala-collection]
            [kenai.utils :refer (link)])
  (:import (org.apache.spark.sql Column
                                 functions)))
(defn ->col-seq
  "Coerce a value into a coll of columns."
  [arg]
  (cond (map? arg)  (for [[k v] arg] (column/as v (name k)))
        (coll? arg) (map column/col arg)
        :else       [(column/col arg)]))

(defn ->col-array
  "Coerce a coll of coerceable values into a coll of columns."
  [args]
  (->> args
       (mapcat ->col-seq)
       (into-array Column)))

(defn % [left-expr right-expr]
  (.mod (column/col left-expr) (column/col right-expr)))
(def mod %)

(defn && [& exprs]
  (reduce #(.and (column/col %1)
                 (column/col %2))
          (column/lit true)
          (->col-array exprs)))

(defn * [& exprs]
  (reduce #(.multiply (column/col %1)
                      (column/col %2))
          (column/lit 1)
          (->col-array exprs)))

(defn + [& exprs]
  (reduce #(.plus (column/col %1)
                  (column/col %2))
          (column/lit 0)
          (->col-array exprs)))

(defn minus [& exprs]
  (reduce #(.minus (column/col %1)
                   (column/col %2))
          (->col-array exprs)))

(defn / [& exprs]
  (reduce #(.divide (column/col %1)
                    (column/col %2))
          (->col-array exprs)))

(defn- compare-columns [compare-fn expr-0 & exprs]
  (let [exprs (-> exprs (conj expr-0))]
    (reduce
     (fn [acc-col [l-expr r-expr]]
       (&& acc-col (compare-fn (column/col l-expr) (column/col r-expr))))
     (column/lit true)
     (map vector exprs (rest exprs)))))

(def === (partial compare-columns #(.equalTo %1 %2)))
(def equal-to ===)

(def <=> (partial compare-columns #(.eqNullSafe %1 %2)))
(def eq-null-safe <=>)

(def =!= (partial compare-columns #(.notEqual %1 %2)))
(def not-equal <=>)

(def < (partial compare-columns #(.lt %1 %2)))
(def lt <)

(def <= (partial compare-columns #(.leq %1 %2)))
(def leq <=)

(def > (partial compare-columns #(.gt %1 %2)))
(def gt >)

(def >= (partial compare-columns #(.geq %1 %2)))
(def geq >=)

(defn bitwise-and [left-expr right-expr]
  (.bitwiseAND (column/col left-expr)
               (column/col right-expr)))

(defn bitwise-or [left-expr right-expr]
  (.bitwiseOR (column/col left-expr)
              (column/col right-expr)))

(defn bitwise-xor [left-expr right-expr]
  (.bitwiseXOR (column/col left-expr)
               (column/col right-expr)))

(defn cast [expr new-type]
  (.cast (column/col expr) new-type))

(defn contains [expr literal]
  (.contains (column/col expr) literal))

(defn ends-with [expr literal]
  (.endsWith (column/col expr) literal))

(defn get-field [expr field-name]
  (.getField (column/col expr) (name field-name)))

(defn get-item [expr k]
  (.getItem (column/col expr) (try (name k) (catch Exception _ k))))

(defn is-in-collection [expr coll]
  (.isInCollection (column/col expr) coll))

(defn is-nan [expr]
  (.isNaN (column/col expr)))

(defn is-not-null [expr]
  (.isNotNull (column/col expr)))

(defn is-null [expr]
  (.isNull (column/col expr)))

(defn isin [expr coll]
  (.isin (column/col expr) (scala-collection/->seq coll)))

(defn like [expr literal]
  (.like (column/col expr) literal))

(defn rlike [expr literal]
  (.rlike (column/col expr) literal))

(defn starts-with [expr literal]
  (.startsWith (column/col expr) literal))

(defn || [& exprs]
  (reduce #(.or (column/col %1)
                (column/col %2))
          (column/lit false)
          (->col-array exprs)))

;;; Sorting Functions
(defn asc [expr] (.asc (column/col expr)))
(defn asc-nulls-first [expr] (.asc_nulls_first (column/col expr)))
(defn asc-nulls-last [expr] (.asc_nulls_last (column/col expr)))
(defn desc [expr] (.desc (column/col expr)))
(defn desc-nulls-first [expr] (.desc_nulls_first (column/col expr)))
(defn desc-nulls-last [expr] (.desc_nulls_last (column/col expr)))

; Java Expressions
(defn between [expr lower-bound upper-bound]
  (.between (column/col expr) lower-bound upper-bound))

; Support Functions
(defn hash-code [expr] (.hashCode (column/col expr)))

; Shortcut Functions
(defn null-rate
  "Aggregate function: returns the null rate of a column."
  [expr]
  (-> expr
      column/col
      is-null
      (cast "int")
      functions/mean
      (.as (clojure.core/str "null_rate(" (name expr) ")"))))

(defn null-count
  "Aggregate function: returns the null count of a column."
  [expr]
  (-> expr
      column/col
      is-null
      (cast "int")
      functions/sum
      (.as (clojure.core/str "null_count(" (name expr) ")"))))

; Aliases
(link bitwise-and &)
(link bitwise-or |)
(link is-nan nan?)
(link is-not-null not-null?)
(link is-null null?)
(link minus -)
