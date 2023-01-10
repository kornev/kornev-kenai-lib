(ns kenai.core
  (:refer-clojure :exclude [filter
                            range
                            -
                            +
                            *
                            /
                            <
                            <=
                            >
                            >=
                            mod
                            cast
                            abs
                            concat
                            flatten
                            hash
                            map
                            not
                            rand
                            reverse
                            second
                            sequence
                            struct
                            when
                            distinct
                            drop
                            empty?
                            group-by
                            sort
                            take
                            count
                            first
                            last
                            max
                            min
                            shuffle
                            replace
                            =
                            boolean
                            byte
                            case
                            cond
                            condp
                            dec
                            double
                            even?
                            float
                            inc
                            int
                            keys
                            long
                            merge
                            neg?
                            odd?
                            pos?
                            rand-nth
                            remove
                            short
                            str
                            vals
                            zero?
                            zipmap])
  (:require [kenai.spark.sql.spark-session :as api-spark-session]
            [kenai.sql.column :as column]
            [kenai.ext.column :as ext-column]
            [kenai.ext.functions :as ext-functions]
            [kenai.ext.dataset :as ext-dataset]
            [kenai.ext.window :as ext-window]
            [kenai.ext.foreign-idioms :as ext-foreign-idioms]
            [kenai.ext.polymorphic :as ext-polymorphic]
            [kenai.ext.clojure-idioms :as ext-clojure-idioms]
            [kenai.sql.data-sources :as data-sources]
            [kenai.rdd.rdd :as rdd]
            [kenai.lifecycle :as lifecycle]
            [kenai.sql.dataset :as dataset]
            [kenai.sql.dataset-creation :as dataset-creation]
            [kenai.spark.sql.dataset :as api-dataset]
            [kenai.spark.sql.row-factory :as api-row-factory]
            [kenai.spark.sql.spark-session :as api-spark-session]
            [kenai.utils :refer (link)])
  (:import (org.apache.spark.sql Dataset
                                 SparkSession)))

;;; lifecycle

(def ^:dynamic ^SparkSession *spark* nil)

(defn app-start!
  "Gets an existing SparkSession or, if there is no existing one, creates a new one based on the options set in this builder."
  [opts]
  (alter-var-root (var *spark*)
                  (constantly (lifecycle/get-or-create opts))))

(defn app-stop!
  "Shut down the Spark context."
  []
  (alter-var-root (var *spark*)
                  (constantly (. *spark* stop))))

(defn app-summary []
  (lifecycle/summary *spark*))

(defn app-settings []
  (lifecycle/settings *spark*))

(defn app-log-level! [level]
  (lifecycle/log-level! *spark* level))

(defn sql [query]
  (api-spark-session/sql *spark* query))

;;; dataset, api-dataset

(link dataset/schema)
(link dataset/filter)
(link dataset/show)
(link dataset/show-vertical)
(link dataset/print-schema)

(defn dataframe [& more]
  (apply dataset/dataframe *spark* more))
(link dataframe df)    ; alias for dataframe

(defn ->df [rdd schema]
  (dataset/dataframe *spark* rdd (dataset/schema schema)))

(defn range ^Dataset [& more]
  (apply api-spark-session/range *spark* more))

;kenai.spark.sql.dataset/columns
;kenai.spark.sql.dataset/->df
;kenai.spark.sql.dataset/->java-rdd

(link api-dataset/->java-rdd ->rdd)
(link ->rdd rdd)  ; alias for toJavaRDD

;;; dataset-creation

(defn table->dataset [xs ys]
  (dataset-creation/table->dataset *spark* xs ys))

(defn map->dataset [m]
  (dataset-creation/map->dataset *spark* m))

(defn records->dataset [xs]
  (dataset-creation/records->dataset *spark* xs))

;;; api-row-factory, api-row

(defn row [& more]
  (api-row-factory/create more))

;kenai.spark.sql.row/->seq

;;; column

(link column/lit)
(link column/col)
(link column/as)

;;; data-sources

(defn read-parquet! [& more]
  (apply data-sources/read-parquet! *spark* more))
(link data-sources/write-parquet!)

(defn read-orc! [& more]
  (apply data-sources/read-orc! *spark* more))
(link data-sources/write-orc!)

(defn read-csv! [& more]
  (apply data-sources/read-csv! *spark* more))
(link data-sources/write-csv!)

(defn read-json! [& more]
  (apply data-sources/read-json! *spark* more))
(link data-sources/write-json!)

(defn read-jdbc! [& more]
  (apply data-sources/read-jdbc! *spark* more))
(link data-sources/write-jdbc!)

(defn read-table! [& more]
  (apply data-sources/read-table! *spark* more))
(link data-sources/write-table!)
(link data-sources/create-temp-view!)
(link data-sources/create-or-replace-temp-view!)
(link data-sources/create-global-temp-view!)
(link data-sources/create-or-replace-global-temp-view!)

;;; catalog

;kenai.spark.sql.catalog/cache-table
;kenai.spark.sql.catalog/clear-cache
;kenai.spark.sql.catalog/current-database
;kenai.spark.sql.catalog/database-exists?
;kenai.spark.sql.catalog/drop-global-temp-view
;kenai.spark.sql.catalog/drop-temp-view
;kenai.spark.sql.catalog/cached?
;kenai.spark.sql.catalog/list-columns
;kenai.spark.sql.catalog/list-databases
;kenai.spark.sql.catalog/list-tables
;kenai.spark.sql.catalog/recover-partitions
;kenai.spark.sql.catalog/refresh-by-path
;kenai.spark.sql.catalog/refresh-table
;kenai.spark.sql.catalog/set-current-database
;kenai.spark.sql.catalog/table-exists?
;kenai.spark.sql.catalog/uncache-table

;;; rdd, api-rdd

(link rdd/storage-level rdd-storage-level)
(link rdd/cache! rdd-cache!)
(link rdd/uncache! rdd-uncache!)
(link rdd/checkpointed? rdd-checkpointed?)
(link rdd/checkpoint! rdd-checkpoint!)
(link rdd/num-partitions rdd-num-partitions)
(link rdd/coalesce rdd-coalesce)
(link rdd/repartition rdd-repartition)
(link rdd/repartition-and-sort-within-partitions rdd-repartition-and-sort-within-partitions)
(link rdd/partition-by rdd-partition-by)
(link rdd/hash-partitioner rdd-hash-partitioner)
(link rdd/map rdd-map)
(link rdd/mapcat rdd-mapcat)
(link rdd-mapcat rdd-flat-map)

;;;; Ext (geni)

;;; Column

(link ext-column/%)
(link ext-column/&)
(link ext-column/&&)
(link ext-column/*)
(link ext-column/+)
(link ext-column/-)
(link ext-column/->col-array)
;(link ext-column/->column)
(link ext-column//)
(link ext-column/<)
(link ext-column/<=)
(link ext-column/<=>)
(link ext-column/=!=)
(link ext-column/===)
(link ext-column/>)
(link ext-column/>=)
(link ext-column/asc)
(link ext-column/asc-nulls-first)
(link ext-column/asc-nulls-last)
(link ext-column/between)
(link ext-column/bitwise-and)
(link ext-column/bitwise-or)
(link ext-column/bitwise-xor)
(link ext-column/cast)
;(link ext-column/col)
(link ext-column/contains)
(link ext-column/desc)
(link ext-column/desc-nulls-first)
(link ext-column/desc-nulls-last)
(link ext-column/ends-with)
(link ext-column/get-field)
(link ext-column/get-item)
(link ext-column/hash-code)
(link ext-column/is-in-collection)
(link ext-column/is-nan)
(link ext-column/is-not-null)
(link ext-column/is-null)
(link ext-column/isin)
(link ext-column/like)
;(link ext-column/lit)
(link ext-column/mod)
(link ext-column/nan?)
(link ext-column/not-null?)
(link ext-column/null-count)
(link ext-column/null-rate)
(link ext-column/null?)
(link ext-column/rlike)
(link ext-column/starts-with)
(link ext-column/|)
(link ext-column/||)

;;; Functions

(link ext-functions/!)
(link ext-functions/**)
(link ext-functions/->date-col)
(link ext-functions/->timestamp-col)
(link ext-functions/->utc-timestamp)
;(link ext-functions/bucket)
;(link ext-functions/days)
;(link ext-functions/hours)
;(link ext-functions/months)
;(link ext-functions/years)
(link ext-functions/abs)
(link ext-functions/acos)
(link ext-functions/add-months)
;(link ext-functions/aggregate)
(link ext-functions/approx-count-distinct)
(link ext-functions/array)
(link ext-functions/array-contains)
(link ext-functions/array-distinct)
(link ext-functions/array-except)
(link ext-functions/array-intersect)
(link ext-functions/array-join)
(link ext-functions/array-max)
(link ext-functions/array-min)
(link ext-functions/array-position)
(link ext-functions/array-remove)
(link ext-functions/array-repeat)
(link ext-functions/array-sort)
(link ext-functions/array-union)
(link ext-functions/arrays-overlap)
(link ext-functions/arrays-zip)
(link ext-functions/ascii)
(link ext-functions/asin)
(link ext-functions/atan)
(link ext-functions/atan-2)
(link ext-functions/atan2)
(link ext-functions/base-64)
(link ext-functions/base64)
(link ext-functions/bin)
(link ext-functions/bitwise-not)
(link ext-functions/broadcast)
(link ext-functions/bround)
(link ext-functions/cbrt)
(link ext-functions/ceil)
(link ext-functions/collect-list)
(link ext-functions/collect-set)
(link ext-functions/concat)
(link ext-functions/concat-ws)
(link ext-functions/conv)
(link ext-functions/cos)
(link ext-functions/cosh)
(link ext-functions/count-distinct)
(link ext-functions/covar)
(link ext-functions/covar-pop)
(link ext-functions/covar-samp)
(link ext-functions/crc-32)
(link ext-functions/crc32)
(link ext-functions/cube-root)
(link ext-functions/cume-dist)
(link ext-functions/current-date)
(link ext-functions/current-timestamp)
(link ext-functions/date-add)
(link ext-functions/date-diff)
(link ext-functions/date-format)
(link ext-functions/date-sub)
(link ext-functions/date-trunc)
(link ext-functions/datediff)
(link ext-functions/day-of-month)
(link ext-functions/day-of-week)
(link ext-functions/day-of-year)
(link ext-functions/dayofmonth)
(link ext-functions/dayofweek)
(link ext-functions/dayofyear)
(link ext-functions/decode)
(link ext-functions/degrees)
(link ext-functions/dense-rank)
(link ext-functions/element-at)
(link ext-functions/encode)
;(link ext-functions/exists)
(link ext-functions/exp)
(link ext-functions/explode)
(link ext-functions/expm-1)
(link ext-functions/expm1)
(link ext-functions/expr)
(link ext-functions/factorial)
(link ext-functions/flatten)
(link ext-functions/floor)
;(link ext-functions/forall)
(link ext-functions/format-number)
(link ext-functions/format-string)
;(link ext-functions/from-csv)
(link ext-functions/from-json)
(link ext-functions/from-unixtime)
(link ext-functions/greatest)
(link ext-functions/grouping)
(link ext-functions/grouping-id)
(link ext-functions/hash)
(link ext-functions/hex)
(link ext-functions/hour)
(link ext-functions/hypot)
(link ext-functions/initcap)
(link ext-functions/input-file-name)
(link ext-functions/instr)
(link ext-functions/kurtosis)
(link ext-functions/lag)
(link ext-functions/last-day)
(link ext-functions/lead)
(link ext-functions/least)
(link ext-functions/length)
(link ext-functions/levenshtein)
(link ext-functions/locate)
(link ext-functions/log)
(link ext-functions/log-10)
(link ext-functions/log-1p)
(link ext-functions/log-2)
(link ext-functions/log10)
(link ext-functions/log1p)
(link ext-functions/log2)
(link ext-functions/lower)
(link ext-functions/lpad)
(link ext-functions/ltrim)
(link ext-functions/map)
(link ext-functions/map-concat)
;(link ext-functions/map-entries)
;(link ext-functions/map-filter)
(link ext-functions/map-from-arrays)
(link ext-functions/map-from-entries)
(link ext-functions/map-keys)
(link ext-functions/map-values)
;(link ext-functions/map-zip-with)
(link ext-functions/md-5)
(link ext-functions/md5)
(link ext-functions/minute)
(link ext-functions/monotonically-increasing-id)
(link ext-functions/month)
(link ext-functions/months-between)
(link ext-functions/nanvl)
(link ext-functions/negate)
(link ext-functions/next-day)
(link ext-functions/not)
(link ext-functions/ntile)
;(link ext-functions/overlay)
(link ext-functions/percent-rank)
(link ext-functions/pi)
(link ext-functions/pmod)
(link ext-functions/posexplode)
(link ext-functions/posexplode-outer)
(link ext-functions/pow)
(link ext-functions/quarter)
(link ext-functions/radians)
(link ext-functions/rand)
(link ext-functions/randn)
(link ext-functions/rank)
(link ext-functions/regexp-extract)
(link ext-functions/regexp-replace)
(link ext-functions/reverse)
(link ext-functions/rint)
(link ext-functions/round)
(link ext-functions/row-number)
(link ext-functions/rpad)
(link ext-functions/rtrim)
;(link ext-functions/schema-of-csv)
;(link ext-functions/schema-of-json)
(link ext-functions/second)
(link ext-functions/sequence)
(link ext-functions/sha-1)
(link ext-functions/sha-2)
(link ext-functions/sha1)
(link ext-functions/sha2)
(link ext-functions/shift-left)
(link ext-functions/shift-right)
(link ext-functions/shift-right-unsigned)
(link ext-functions/signum)
(link ext-functions/sin)
(link ext-functions/sinh)
(link ext-functions/size)
(link ext-functions/skewness)
(link ext-functions/slice)
(link ext-functions/sort-array)
(link ext-functions/soundex)
(link ext-functions/spark-partition-id)
(link ext-functions/split)
(link ext-functions/sqr)
(link ext-functions/sqrt)
(link ext-functions/std)
(link ext-functions/stddev)
(link ext-functions/stddev-pop)
(link ext-functions/stddev-samp)
(link ext-functions/struct)
(link ext-functions/substring)
(link ext-functions/substring-index)
(link ext-functions/sum-distinct)
(link ext-functions/tan)
(link ext-functions/tanh)
(link ext-functions/time-window)
;(link ext-functions/to-csv)
(link ext-functions/to-date)
(link ext-functions/to-timestamp)
(link ext-functions/to-utc-timestamp)
;(link ext-functions/transform)
;(link ext-functions/transform-keys)
;(link ext-functions/transform-values)
(link ext-functions/translate)
(link ext-functions/trim)
(link ext-functions/unbase-64)
(link ext-functions/unbase64)
(link ext-functions/unhex)
(link ext-functions/unix-timestamp)
(link ext-functions/upper)
(link ext-functions/var-pop)
(link ext-functions/var-samp)
(link ext-functions/variance)
(link ext-functions/weekofyear)
(link ext-functions/week-of-year)
(link ext-functions/when)
;(link ext-functions/xxhash-64)
;(link ext-functions/xxhash64)
(link ext-functions/year)
;(link ext-functions/zip-with)

;;; Dataset

(link ext-dataset/add)
(link ext-dataset/agg)
(link ext-dataset/agg-all)
(link ext-dataset/approx-quantile)
(link ext-dataset/bit-size)
(link ext-dataset/bloom-filter)
(link ext-dataset/cache)
(link ext-dataset/checkpoint)
(link ext-dataset/col-regex)
(link ext-dataset/collect)
(link ext-dataset/collect-col)
(link ext-dataset/collect-vals)
(link ext-dataset/column-names)
(link ext-dataset/columns)
(link ext-dataset/compatible?)
(link ext-dataset/confidence)
(link ext-dataset/count-min-sketch)
(link ext-dataset/cov)
(link ext-dataset/cross-join)
(link ext-dataset/crosstab)
(link ext-dataset/cube)
(link ext-dataset/depth)
(link ext-dataset/describe)
(link ext-dataset/distinct)
(link ext-dataset/drop)
(link ext-dataset/drop-duplicates)
(link ext-dataset/drop-na)
(link ext-dataset/dtypes)
(link ext-dataset/empty?)
(link ext-dataset/estimate-count)
(link ext-dataset/except)
(link ext-dataset/except-all)
(link ext-dataset/expected-fpp)
(link ext-dataset/fill-na)
(link ext-dataset/first-vals)
(link ext-dataset/freq-items)
(link ext-dataset/group-by)
(link ext-dataset/head)
(link ext-dataset/head-vals)
(link ext-dataset/hint)
(link ext-dataset/input-files)
(link ext-dataset/intersect)
(link ext-dataset/intersect-all)
(link ext-dataset/is-compatible)
(link ext-dataset/is-empty)
(link ext-dataset/is-local)
(link ext-dataset/is-streaming)
(link ext-dataset/join)
(link ext-dataset/join-with)
(link ext-dataset/last-vals)
(link ext-dataset/limit)
(link ext-dataset/local?)
(link ext-dataset/merge-in-place)
(link ext-dataset/might-contain)
(link ext-dataset/order-by)
(link ext-dataset/partitions)
(link ext-dataset/persist)
(link ext-dataset/pivot)
;(link ext-dataset/print-schema)
(link ext-dataset/put)
(link ext-dataset/random-split)
;(link ext-dataset/rdd)
(link ext-dataset/relative-error)
(link ext-dataset/rename-columns)
(link ext-dataset/repartition)
(link ext-dataset/repartition-by-range)
(link ext-dataset/replace-na)
(link ext-dataset/rollup)
(link ext-dataset/sample)
;(link ext-dataset/sample-by)
(link ext-dataset/select)
(link ext-dataset/select-expr)
;(link ext-dataset/show)
;(link ext-dataset/show-vertical)
(link ext-dataset/sort)
(link ext-dataset/sort-within-partitions)
(link ext-dataset/spark-session)
(link ext-dataset/sql-context)
(link ext-dataset/storage-level)
(link ext-dataset/streaming?)
(link ext-dataset/summary)
(link ext-dataset/tail)
(link ext-dataset/tail-vals)
(link ext-dataset/take)
(link ext-dataset/take-vals)
(link ext-dataset/to-byte-array)
(link ext-dataset/total-count)
(link ext-dataset/union)
(link ext-dataset/union-by-name)
(link ext-dataset/unpersist)
(link ext-dataset/width)
(link ext-dataset/with-column)
(link ext-dataset/with-column-renamed)

;;; Window

(link ext-window/over)
(link ext-window/unbounded-following)
(link ext-window/unbounded-preceding)
(link ext-window/window)
(link ext-window/windowed)

;;; Polymorphic

;(link ext-polymorphic/alias)
;(link ext-polymorphic/as)
;(link ext-polymorphic/assoc
(link ext-polymorphic/coalesce)
;(link ext-polymorphic/corr
(link ext-polymorphic/count)
;(link ext-polymorphic/dissoc
(link ext-polymorphic/explain)
;(link ext-polymorphic/filter
(link ext-polymorphic/first)
(link ext-polymorphic/interquartile-range)
(link ext-polymorphic/iqr)
(link ext-polymorphic/last)
(link ext-polymorphic/max)
(link ext-polymorphic/mean)
(link ext-polymorphic/median)
(link ext-polymorphic/min)
(link ext-polymorphic/quantile)
(link ext-polymorphic/shuffle)
(link ext-polymorphic/sum)
;(link ext-polymorphic/to-df)
(link ext-polymorphic/to-json)
;(link ext-polymorphic/update)
;(link ext-polymorphic/where)

;;; Foreign idioms

;(link ext-foreign-idioms/->dataset)
(link ext-foreign-idioms/clip)
(link ext-foreign-idioms/cut)
;(link ext-foreign-idioms/name-value-seq->dataset)
(link ext-foreign-idioms/nlargest)
(link ext-foreign-idioms/nsmallest)
(link ext-foreign-idioms/nunique)
;(link ext-foreign-idioms/qcut)
(link ext-foreign-idioms/random-choice)
(link ext-foreign-idioms/random-exp)
(link ext-foreign-idioms/random-int)
(link ext-foreign-idioms/random-norm)
(link ext-foreign-idioms/random-uniform)
(link ext-foreign-idioms/rchoice)
(link ext-foreign-idioms/replace)
(link ext-foreign-idioms/rexp)
(link ext-foreign-idioms/rnorm)
(link ext-foreign-idioms/runif)
(link ext-foreign-idioms/runiform)
(link ext-foreign-idioms/select-columns)
(link ext-foreign-idioms/shape)
(link ext-foreign-idioms/value-counts)

;;; Clojure idioms

(link ext-clojure-idioms/=)
(link ext-clojure-idioms/boolean)
(link ext-clojure-idioms/byte)
(link ext-clojure-idioms/case)
(link ext-clojure-idioms/cond)
(link ext-clojure-idioms/condp)
(link ext-clojure-idioms/dec)
(link ext-clojure-idioms/double)
(link ext-clojure-idioms/even?)
(link ext-clojure-idioms/float)
(link ext-clojure-idioms/if)
(link ext-clojure-idioms/inc)
(link ext-clojure-idioms/int)
(link ext-clojure-idioms/keys)
(link ext-clojure-idioms/long)
(link ext-clojure-idioms/merge)
;(link ext-clojure-idioms/merge-with)
(link ext-clojure-idioms/neg?)
(link ext-clojure-idioms/odd?)
(link ext-clojure-idioms/pos?)
(link ext-clojure-idioms/rand-nth)
(link ext-clojure-idioms/remove)
;(link ext-clojure-idioms/rename-keys)
;(link ext-clojure-idioms/select-keys)
(link ext-clojure-idioms/short)
(link ext-clojure-idioms/str)
(link ext-clojure-idioms/vals)
(link ext-clojure-idioms/zero?)
(link ext-clojure-idioms/zipmap)

;;; Other

(def to-debug-string
  "Coerce to string useful for debugging."
  (memfn toDebugString))
(link to-debug-string ->debug-string)
