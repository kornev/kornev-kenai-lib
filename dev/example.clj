(ns example
  (:require [kenai.sql.row :as row]
            [kenai.rdd.rdd :as r]
            [kenai.core :as k]))

(defn path [s]
      (str (System/getProperty "user.dir") s))

(k/app-start! {:app-name (str "embedded-" (rand-int 99999))
               :settings {:spark.sql.warehouse.dir (path "/apps/spark/warehouse")
                          :hive.metastore.warehouse.dir (path "/apps/spark/warehouse")
                          :spark.sql.hive.metastore.jars "builtin"}
               :checkpoint-dir (path "/apps/spark/checkpoint")})

(k/sql "CREATE DATABASE report")
(k/sql "USE report")
(k/sql "CREATE TABLE IF NOT EXISTS sales (
            `channel` STRING,
            `genre`   MAP<STRING, ARRAY<INT>>,
            `flight`  MAP<INT, ARRAY<INT>>
        )
        PARTITIONED BY (`dt` STRING)
        STORED AS PARQUET")
(k/sql "INSERT INTO TABLE sales PARTITION(dt='2020-10-08')
        VALUES ('MATCH TV',map('adult',array(1601330359,1601330990,1601330348)),map(340405,array(1601330359,1601330990,1601330348))),
               ('TNT',map('adult',array(1600455849),'health',array(1600455849)),map(340405,array(1600455849))),
               ('ZVEZDA',map('adult',array(1601043382),'sport',array(1601043382)),map(340405,array(1601043382)))")

(-> (k/read-table! "report" "sales")
    (k/select (k/as :channel :channel_id)
              (k/as (k/explode :genre) :break_type :break_flight_start))
    (k/create-or-replace-temp-view! "programme_genre"))

(-> (k/read-table! "report.sales")
    (k/select (k/as :channel :channel_id)
              (k/as (k/explode :flight) :break_id :break_flight_start))
    (k/create-or-replace-temp-view! "programme_flight"))

(-> (k/sql "SELECT s.channel_id
                 , s.break_type
                 , c.break_id
              FROM programme_genre AS s INNER JOIN programme_flight AS c
                ON s.channel_id = c.channel_id")
    (k/agg {:all_break_ids (k/collect-set :break_id)})
    (k/create-or-replace-temp-view! "sales_affinity"))

(-> (k/read-table! "sales_affinity")
    (k/->rdd)
    (r/map #(-> (row/get-list % :all_break_ids) first k/row))
    (k/->df {:break_id :int})
    (k/collect)
    (first)
    (:break_id))

(k/sql "DROP DATABASE report CASCADE")
(k/app-stop!)
