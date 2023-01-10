(ns kenai.spark.sql.catalog
  (:require [kenai.spark.storage :as storage])
  (:import (org.apache.spark.sql.catalog Catalog)))

(defn cache-table
  "Caches the specified table with the given storage level."
  ([^Catalog catalog ^String table-name]
   (cache-table catalog table-name :memory-and-disk))
  ([^Catalog catalog ^String table-name level]
   {:pre [(contains? storage/storage-levels level)]}
   (. catalog cacheTable table-name (storage/storage-levels level))))

(defn clear-cache
  "Removes all cached tables from the in-memory cache."
  [^Catalog catalog]
  (. catalog clearCache))

(defn current-database
  "Returns the current default database in this session."
  [^Catalog catalog]
  (. catalog currentDatabase))

(defn database-exists?
  "Check if the database with the specified name exists."
  [^Catalog catalog ^String db-name]
  (. catalog databaseExists db-name))

(defn drop-global-temp-view
  "Drops the global temporary view with the given view name in the catalog."
  [^Catalog catalog ^String view-name]
  (. catalog dropGlobalTempView view-name))

(defn drop-temp-view
  "Drops the local temporary view with the given view name in the catalog."
  [^Catalog catalog ^String view-name]
  (. catalog dropTempView view-name))

(defn cached?
  "Returns true if the table is currently cached in-memory."
  [^Catalog catalog ^String table-name]
  (. catalog isCached table-name))

(defn list-columns
  "Returns a list of columns for the given table/view in the specified database."
  ([^Catalog catalog ^String table-name]
   (. catalog listColumns table-name))
  ([^Catalog catalog ^String db-name ^String table-name]
   (. catalog listColumns db-name table-name)))

(defn list-databases
  "Returns a list of databases available across all sessions."
  [^Catalog catalog]
  (. catalog listDatabases))

(defn list-tables
  "Returns a list of tables/views in the specified database."
  ([^Catalog catalog]
   (. catalog listTables))
  ([^Catalog catalog ^String db-name]
   (. catalog listTables db-name)))

(defn recover-partitions
  "Recovers all the partitions in the directory of a table and update the catalog."
  [^Catalog catalog ^String table-name]
  (. catalog recoverPartitions table-name))

(defn refresh-by-path
  "Invalidates and refreshes all the cached data (and the associated metadata) for any Dataset that contains the given data source path."
  [^Catalog catalog ^String path]
  (. catalog refreshByPath path))

(defn refresh-table
  "Invalidates and refreshes all the cached data and metadata of the given table."
  [^Catalog catalog ^String table-name]
  (. catalog refreshTable table-name))

(defn set-current-database
  "Sets the current default database in this session."
  [^Catalog catalog ^String db-name]
  (. catalog setCurrentDatabase db-name))

(defn table-exists?
  "Check if the table or view with the specified name exists in the specified database."
  ([^Catalog catalog ^String table-name]
   (. catalog tableExists table-name))
  ([^Catalog catalog ^String db-name ^String table-name]
   (. catalog tableExists db-name table-name)))

(defn uncache-table
  "Removes the specified table from the in-memory cache."
  [^Catalog catalog ^String table-name]
  (. catalog uncacheTable table-name))
