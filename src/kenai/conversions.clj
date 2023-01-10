(ns kenai.conversions)

(defn fmap [fn coll]  ; traversable
  (into (empty coll) (map fn coll)))

(defn lift-coll [x]
  (if (or (coll? x) (nil? x)) x [x]))
