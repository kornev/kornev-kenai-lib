(ns kenai.scala.collection
  (:import (scala.collection JavaConversions)
           (scala.reflect ClassTag$)))

(defn- as-scala-iterator
  "Converts a Clojure collection to a Scala iterator."
  ^scala.collection.Iterator [^java.lang.Iterable coll]
  (JavaConversions/asScalaIterator (. coll iterator)))

(defn ->seq
  "Converts a Clojure collection to a Scala Seq."
  ^scala.collection.Seq [coll]
  (. (as-scala-iterator coll) toSeq))

(defn ->array
  "Converts a Clojure collection to a Scala Array.
  Like Clojure core into-array, all elements must be of the same class (or nil)."
  ^scala.Array [^Class cls coll]
  (. (->seq coll) toArray (. ClassTag$/MODULE$ apply cls)))

(defn ->clj-seq
  "Converts a Scala Seq to a clojure seq."
  [^scala.collection.Seq xs]
  (if (. xs isEmpty)
    '()
    (seq (JavaConversions/asJavaIterable xs))))
