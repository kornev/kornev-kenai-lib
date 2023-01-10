(ns kenai.utils)

(defn refresh-link
  [src dst]
  (add-watch src dst
             (fn [link target _ new-state]
               (alter-var-root link (constantly new-state))
               (alter-meta! link merge (dissoc (meta target) :name)))))

(defmacro link
  "Make links between functions."
  ([target]
   `(link ~target nil))
  ([target link-name]
   (let [v (resolve target)
         m (meta v)
         n (or link-name (:name m))]
     (when-not v
       (throw (IllegalArgumentException. (str "Don't recognize " target))))
     (when (:macro m)
       (throw (IllegalArgumentException. (str "Calling link on a macro: " target))))
     `(do
        (def ~n (deref ~v))
        (alter-meta! (var ~n) merge (dissoc (meta ~v) :name))
        (refresh-link ~v (var ~n))
        ~v))))
