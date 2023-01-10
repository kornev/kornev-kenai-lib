(ns kenai.ext.clojure-idioms
  (:refer-clojure :exclude [=
                            boolean
                            byte
                            case
                            cond
                            condp
                            dec
                            double
                            even?
                            float
                            if
                            inc
                            int
                            keys
                            long
                            merge
                            merge-with
                            neg?
                            odd?
                            pos?
                            rand-nth
                            remove
                            rename-keys
                            select-keys
                            short
                            str
                            vals
                            zero?
                            zipmap])
  (:require [kenai.utils :refer (link)]
            [kenai.ext.functions :as sql]
            [kenai.ext.polymorphic :as polymorphic]
            [kenai.sql.column :as column]
            [kenai.ext.column :as ext-column]
            [kenai.ext.dataset :as dataset])
  (:import
   (org.apache.spark.sql functions)))

;; Start patch
(link column/col ->column)
;; End patch

;; Collections
(defn remove
  "Returns a new Dataset that only contains elements where func returns false."
  [dataframe expr]
  (.filter dataframe (-> expr ->column (.cast "boolean") functions/not)))

(defn rand-nth
  "Returns a random row collected."
  [dataframe]
  (let [small-frac (min 1.0 (/ 10.0 (.count dataframe)))]
    (-> dataframe (dataset/sample small-frac) (dataset/limit 1) dataset/head)))

;; Arithmetic
(defn inc
  "Returns an expression one greater than `expr`."
  [expr]
  (ext-column/+ (->column expr) 1))

(defn dec
  "Returns an expression one less than `expr`."
  [expr]
  (ext-column/- (->column expr) 1))

;; Casting
(defn short
  "Casts the column to a short."
  [expr]
  (ext-column/cast (->column expr) "short"))

(defn int
  "Casts the column to an int."
  [expr]
  (ext-column/cast (->column expr) "int"))

(defn long
  "Casts the column to a long."
  [expr]
  (ext-column/cast (->column expr) "long"))

(defn float
  "Casts the column to a float."
  [expr]
  (ext-column/cast (->column expr) "float"))

(defn double
  "Casts the column to a double."
  [expr]
  (ext-column/cast (->column expr) "double"))

(defn boolean
  "Casts the column to a boolean."
  [expr]
  (ext-column/cast (->column expr) "boolean"))

(defn byte
  "Casts the column to a byte."
  [expr]
  (ext-column/cast (->column expr) "byte"))

(defn str
  "Casts the column to a str."
  [expr]
  (ext-column/cast (->column expr) "string"))

;; Predicates
(defn zero?
  "Returns true if `expr` is zero, else false."
  [expr]
  (ext-column/=== (->column expr) 0))

(defn pos?
  "Returns true if `expr` is greater than zero, else false."
  [expr]
  (ext-column/< 0 (->column expr)))

(defn neg?
  "Returns true if `expr` is less than zero, else false."
  [expr]
  (ext-column/< (->column expr) 0))

(defn even?
  "Returns true if `expr` is even, else false."
  [expr]
  (ext-column/=== (ext-column/mod (->column expr) 2) 0))

(defn odd?
  "Returns true if `expr` is odd, else false."
  [expr]
  (ext-column/=== (ext-column/mod (->column expr) 2) 1))

(link ext-column/=== =)

;; Map Operations
(defn merge
  "Variadic version of `map-concat`."
  [expr & ms]
  (reduce sql/map-concat expr ms))

(defn- rename-cols
  "Returns a new Dataset with columns renamed according to `kmap`."
  [k kmap]
  (concat
   (map
    (fn [[old-k new-k]]
      (sql/when (.equalTo (->column k) (->column old-k))
        (->column new-k)))
    kmap)
   [(->column k)]))

;; (defn rename-keys
;;   "Same as `transform-keys` with a map arg."
;;   [expr kmap]
;;   (sql/transform-keys
;;    expr
;;    (fn [k _] (functions/coalesce (column/->col-array (rename-cols k kmap))))))

;; (defn select-keys
;;   "Returns a map containing only those entries in map (`expr`) whose key is in `ks`."
;;   [expr ks]
;;   (sql/map-filter expr (fn [k _] (.isin k (interop/->scala-seq ks)))))

(link sql/map-from-arrays zipmap)
(link sql/map-keys keys)
(link sql/map-values vals)
;; (import-fn sql/map-zip-with merge-with)

;; Common Macros
(defn cond
  "Returns a new Column imitating Clojure's `cond` macro behaviour."
  [& clauses]
  (let [predicates   (take-nth 2 clauses)
        then-cols    (take-nth 2 (rest clauses))
        whenned-cols (map (fn [pred then]
                            ;; clojure.core/if not available for some reason.
                            ;; this is a workaround using a map lookup with a default.
                            ({:else (->column then)} pred (sql/when pred then)))
                          predicates
                          then-cols)]
    (apply polymorphic/coalesce whenned-cols)))

(defn condp
  "Returns a new Column imitating Clojure's `condp` macro behaviour."
  [pred expr & clauses]
  (let [default      (when (clojure.core/odd? (count clauses))
                       (last clauses))
        test-exprs   (take-nth 2 clauses)
        then-cols    (take-nth 2 (rest clauses))
        whenned-cols (map #(sql/when
                            (pred (->column %1)
                                  (->column expr))
                             %2)
                          test-exprs
                          then-cols)]
    (apply polymorphic/coalesce (concat whenned-cols [(->column default)]))))

(defn case
  "Returns a new Column imitating Clojure's `case` macro behaviour."
  [expr & clauses]
  (let [default    (when (clojure.core/odd? (count clauses))
                     (last clauses))
        match-cols (take-nth 2 clauses)
        then-cols  (take-nth 2 (rest clauses))
        whenned-cols (map #(sql/when (ext-column/=== %1 expr) %2) match-cols then-cols)]
    (apply polymorphic/coalesce (concat whenned-cols [(->column default)]))))

(link sql/when if)
