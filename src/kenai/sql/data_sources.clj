(ns kenai.sql.data-sources
  (:refer-clojure :exclude [partition-by])
  (:require [clojure.string :as str]
            [camel-snake-kebab.core :as word]
            [kenai.spark.sql.dataset :as api-dataset]
            [kenai.spark.sql.data-frame-reader :as api-reader]
            [kenai.spark.sql.data-frame-writer :as api-writer]
            [kenai.sql.dataset :as dataset]
            [kenai.conversions :refer (fmap lift-coll)])
  (:import (java.text Normalizer
                      Normalizer$Form)
           (org.apache.spark.sql SparkSession
                                 Dataset
                                 DataFrameReader
                                 DataFrameWriter)))

(def default-options
  "Default DataFrameReader options."
  {"csv" {:header "true" :infer-schema "true"}})

(def ^:private opts-like
  (partial fmap (fn [[k v]] [(word/->camelCase (name k)) (str v)])))

(defn- configure-reader
  ^DataFrameReader [^DataFrameReader r m]
  (api-reader/options r (opts-like m)))

(defn- configure-writer
  ^DataFrameWriter [^DataFrameWriter w m]
  (api-writer/options w (opts-like m)))

(defn- remove-punctuations [s]
  (str/replace s #"[.,\/#!$%\^&\*;:{}=\`~()°]" ""))

(defn- deaccent
  "Replace accented characters with their unicode equivalent."
  [s]
  (-> (Normalizer/normalize s Normalizer$Form/NFD)
      (str/replace #"\p{InCombiningDiacriticalMarks}+" "")))

(defn rename-to-kebab-columns
  "Returns a new Dataset with all columns renamed to kebab cases."
  [x]
  (->> (api-dataset/columns x)
       (map remove-punctuations)
       (map deaccent)
       (map word/->kebab-case)
       (api-dataset/->df x)))

(defn- read-data!
  [^SparkSession spark
   ^String format-name
   ^String path
   options]
  (let [reader-opts (dissoc options :kebab-columns :schema)
        defaults    (default-options format-name)
        schema      (:schema options)
        reader      (-> (.. spark read (format format-name))
                        (configure-reader (merge defaults reader-opts))
                        (cond-> (not (nil? schema))
                          (api-reader/schema (dataset/schema schema))))]
    (-> (. reader load path)
        (cond-> (:kebab-columns options) rename-to-kebab-columns))))

(defn- configure-base-writer
  ^DataFrameWriter [^DataFrameWriter writer options]
  (let [mode         (:mode options)
        partition-id (->> (:partition-by options)
                          (lift-coll)
                          (map name))
        writer       (-> writer
                         (cond-> mode (api-writer/mode mode))
                         (cond-> partition-id (api-writer/partition-by partition-id)))]
    (configure-writer writer (dissoc options :mode :partition-by))))

(defn- write-data!
  [format ^Dataset dataframe path options]
  (-> (. dataframe write)
      (. format format)
      (configure-base-writer options)
      (. save path)))

(defn read-parquet!
  "Loads a Parquet file and returns the results as a DataFrame.
  See: https://spark.apache.org/docs/2.4.8/sql-data-sources-parquet.html"
  ^Dataset
  ([spark path]
   (read-parquet! spark path {}))
  ([spark path options]
   (read-data! spark "parquet" path options)))

(defn write-parquet!
  "Saves the content of the DataFrame in Parquet format at the specified path.
  See: https://spark.apache.org/docs/2.4.8/sql-data-sources-parquet.html"
  ([dataframe path]
   (write-parquet! dataframe path {}))
  ([dataframe path options]
   (write-data! "parquet" dataframe path options)))

(defn read-orc!
  "Loads an ORC file and returns the result as a DataFrame.
  See: https://spark.apache.org/docs/2.4.8/sql-data-sources-orc.html"
  ^Dataset
  ([spark path]
   (read-orc! spark path {}))
  ([spark path options]
   (read-data! spark "orc" path options)))

(defn write-orc!
  "Saves the content of the DataFrame in ORC format at the specified path.
  See: https://spark.apache.org/docs/2.4.8/sql-data-sources-orc.html"
  ([dataframe path]
   (write-orc! dataframe path {}))
  ([dataframe path options]
   (write-data! "orc" dataframe path options)))

(defn read-csv!
  "Loads a CSV file and returns the results as a DataFrame."
  ^Dataset
  ([spark path]
   (read-csv! spark path {}))
  ([spark path options]
   (read-data! spark "csv" path options)))

(defn write-csv!
  "Saves the content of the DataFrame in CSV format at the specified path."
  ([dataframe path]
   (write-csv! dataframe path {}))
  ([dataframe path options]
   (write-data! "csv" dataframe path options)))

(defn read-json!
  "Loads a JSON file and returns the results as a DataFrame.
  See: https://spark.apache.org/docs/2.4.8/sql-data-sources-json.html"
  ^Dataset
  ([spark path]
   (read-csv! spark path {}))
  ([spark path options]
   (read-data! spark "json" path options)))

(defn write-json!
  "Saves the content of the DataFrame in JSON format at the specified path.
  See: https://spark.apache.org/docs/2.4.8/sql-data-sources-json.html"
  ([dataframe path]
   (write-csv! dataframe path {}))
  ([dataframe path options]
   (write-data! "json" dataframe path options)))

(defn read-jdbc!
  "Construct a DataFrame representing the database table accessible via JDBC URL url named table and connection properties.
  See: https://spark.apache.org/docs/2.4.8/sql-data-sources-jdbc.html"
  ^Dataset [^SparkSession spark options]
  (-> (.. spark read (format "jdbc"))
      (configure-reader options)
      (. load)))

(defn write-jdbc!
  "Saves the content of the DataFrame to an external database table via JDBC.
  See: https://spark.apache.org/docs/2.4.8/sql-data-sources-jdbc.html"
  [^Dataset dataframe options]
  (let [mode                (:mode options)
        unconfigured-writer (-> (. dataframe write)
                                (. format "jdbc")
                                (cond-> mode (api-writer/mode mode)))
        configured-writer   (configure-writer unconfigured-writer (dissoc options :mode))]
    (. configured-writer save)))

(defn read-table!
  "Returns the specified table/view as a DataFrame."
  (^Dataset [^SparkSession spark ^String table-name]
   (. spark table table-name))
  (^Dataset [^SparkSession spark ^String db-name ^String tbl-name]
   (read-table! spark (str db-name "." tbl-name))))

(defn write-table!
  "Saves the content of the DataFrame as the specified table."
  ([^Dataset dataframe ^String table-name]
   (write-table! dataframe table-name {}))
  ([^Dataset dataframe ^String table-name options]
   (-> (. dataframe write)
       (configure-base-writer options)
       (. saveAsTable table-name))))

(defn create-temp-view!
  "Creates a local temporary view using the given name."
  [^Dataset dataframe ^String view-name]
  (. dataframe createTempView view-name))

(defn create-or-replace-temp-view!
  "Creates or replaces a local temporary view using the given name."
  [^Dataset dataframe ^String view-name]
  (. dataframe createOrReplaceTempView view-name))

(defn create-global-temp-view!
  "Creates a global temporary view using the given name."
  [^Dataset dataframe ^String view-name]
  (. dataframe createGlobalTempView view-name))

(defn create-or-replace-global-temp-view!
  "Creates or replaces a global temporary view using the given name."
  [^Dataset dataframe ^String view-name]
  (. dataframe createOrReplaceGlobalTempView view-name))
