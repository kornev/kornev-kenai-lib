(ns kenai.scala.option
  (:import (scala Option
                  Some)))

(defn get-or-nil
  "Get the value of the Option or returns nil if option is empty."
  [^Option x]
  (when (instance? Some x)
    (. ^Some x get)))
