(ns dev
  (:require [clojure.string :as str]
            [clojure.reflect :refer (reflect)]
            [clj-java-decompiler.core :refer (decompile)]
            [fipp.edn :refer (pprint) :rename {pprint echo}]))
