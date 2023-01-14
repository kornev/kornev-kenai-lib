(defproject com.ensime/kenai "0.2"
  :description "Kenai is a package that provides a lightweight frontend to use Apache Spark from Clojure"
  :url         "https://github.com/kornev/kornev-kenai-lib"
  :license {:name "MIT"
            :url  "https://opensource.org/licenses/MIT"}

  :dependencies [[camel-snake-kebab/camel-snake-kebab "0.4.3"]
                 [org.clojure/tools.logging "1.2.4"]
                 [com.taoensso/nippy "3.2.0"]
                 [org.clojure/java.data "1.0.95"]
                 [zero.one/fxl "0.0.7"]
                 [potemkin "0.4.6"]]

  :java-source-paths ["src-native"]
  :javac-options     ["-target" "1.8" "-source" "1.8"]

  :source-paths   ["src"]
  :resource-paths ["src-resources"]
  :test-paths     ["test"]

  :profiles {:provided
             {:dependencies [[org.clojure/clojure "1.11.1"]]}

             :spark-2.4
             ^{:pom-scope :provided}
             {:dependencies [[org.apache.spark/spark-core_2.11 "2.4.8"]
                             [org.apache.spark/spark-hive_2.11 "2.4.8"]]}

             :dev
             {:jvm-opts ["-Dderby.stream.error.file=apps/hive/derby.log"]
              :resource-paths ["test-resources"]}

             :test
             {:jvm-opts ["-Dderby.stream.error.file=apps/hive/derby.log"
                         "-XX:-OmitStackTraceInFastThrow"]
              :dependencies [[midje "1.10.9"]]}

             :repl
             {:source-paths ["dev"]
              :plugins [[cider/cider-nrepl "0.28.7"]]
              :dependencies [[com.clojure-goes-fast/clj-java-decompiler "0.3.3"]
                             [fipp/fipp "0.6.26"]
                             [rm-hull/table "0.7.1"]]}}

  :global-vars {*warn-on-reflection* true}

  :plugins [[lein-ancient "0.7.0"]
            [lein-cloverage "1.2.4"]
            [lein-cljfmt "0.9.0"]])
