(ns datahike-sync.core
  (:require [datahike.api :as d]
            [clojure.core.async :refer [<! go-loop >! timeout chan put!]]))

(def origin-dir "/tmp/origin-dat")
(def origin-uri (str "datahike:file://" origin-dir))

(def schema [{:db/ident :name
              :db/valueType :db.type/string
              :db/cardinality :db.cardinality/one}])

(comment
  ;; cleanup database if necessary
  (d/delete-database origin-uri)

  ;; create a database at this place
  (d/create-database origin-uri :initial-tx schema)

  )

(def origin-conn (d/connect origin-uri))

(defn get-all-names [conn]
  (d/q '[:find ?n
         :where [?e :name ?n]]
       @conn))

;; lets add some data and wait for the transaction
(d/transact origin-conn [{:name "Christian"}
                         {:name "Judith"}
                         {:name "Konrad"}])

(get-all-names origin-conn)
;; => #{["Konrad"] ["Christian"] ["Judith"]}
;;
(def clone-dir "/tmp/clone-dat")
(def clone-uri (str "datahike:file://" clone-dir))

;; create a dat in the origin-dir directory and share it. Then clone the dat to the clone-dir directory. This should take a second or two.

;; connect to cloned connection
(def clone-conn (d/connect clone-uri))

;; let's check the cloned data
(get-all-names clone-conn)
;; => #{["Konrad"] ["Christian"] ["Judith"]}
;;
;; add new data to the origin database
(d/transact origin-conn [{:name "Pablo"}])

;; check new data in the origin
(get-all-names origin-conn)
;; => #{["Konrad"] ["Pablo"] ["Christian"] ["Judith"]}

;; check new data in the clone, "Pablo" is missing
(get-all-names clone-conn)
;; => #{["Konrad"] ["Christian"] ["Judith"]}

;; reconnect clone
(def clone-conn (d/connect clone-uri))

;; check clone again
(get-all-names clone-conn)
;; => #{["Konrad"] ["Pablo"] ["Christian"] ["Judith"]}

;; watcher function for changes in dat signatures
(defn reconnect []
  (let [state (atom {:chan (chan)
                     :conn (d/connect clone-uri)})]
    (go-loop [event :reconnect]
      (case event
        :stop (println :stopping)
        :reconnect (do
                     (swap! state assoc :conn (d/connect clone-uri))
                     (println :reconnected)
                     (recur (<! (:chan @state))))
        (recur (<! (:chan @state)))))
    (go-loop [l (.length (clojure.java.io/file (str clone-dir "/.dat/content.signatures")))]
      (<! (timeout 2000))
      (let [new-l (.length (clojure.java.io/file (str clone-dir "/.dat/content.signatures")))]
        (when-not (= l new-l)
          (>! (:chan @state) :reconnect))
        (recur new-l)))
    state))

(def state (reconnect))

;; add new data to origin and check it
(d/transact origin-conn [{:name "Chrislain"}])

(get-all-names origin-conn)
;; => #{["Konrad"] ["Chrislain"] ["Pablo"] ["Christian"] ["Judith"]}

;; you'll see the :reconnect message once a the connection was refreshed
(get-all-names (:conn @state))
;; => #{["Konrad"] ["Chrislain"] ["Pablo"] ["Christian"] ["Judith"]}

;; let's try again!
(d/transact origin-conn [{:name "Freddy"}])

(get-all-names origin-conn)
;; => #{["Konrad"] ["Chrislain"] ["Pablo"] ["Freddy"] ["Christian"] ["Judith"]}

(get-all-names (:conn @state))
;; => #{["Konrad"] ["Chrislain"] ["Pablo"] ["Freddy"] ["Christian"] ["Judith"]}

;; stop watching
(put! (:chan @state) :stop)
