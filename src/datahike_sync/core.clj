(ns datahike-sync.core
  (:require [datahike.api :as d]
            [clojure.core.async :refer [<! go-loop >! timeout chan put!]]))

(def origin-dir "/tmp/origin-dat")
(def origin-uri (str "datahike:file://" origin-dir))

(def schema [{:db/ident :name
              :db/valueType :db.type/string
              :db/cardinality :db.cardinality/one}
             {:db/ident :age
              :db/valueType :db.type/long
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
(d/transact origin-conn [{:name "Alice" :age 33}
                         {:name "Bob" :age 37}
                         {:name "Charlie" :age 55}])

(get-all-names origin-conn)
;; => #{["Charlie"] ["Alice"] ["Bob"]}

(def clone-dir "/tmp/clone-dat")
(def clone-uri (str "datahike:file://" clone-dir))

;; create a dat in the origin-dir directory and share it. Then clone the dat to the clone-dir directory. This should take a second or two.

;; connect to cloned connection
(def clone-conn (d/connect clone-uri))

;; let's check the cloned data
(get-all-names clone-conn)
;; => #{["Charlie"] ["Alice"] ["Bob"]}


;; add new data to the origin database
(d/transact origin-conn [{:name "Dorothy"}])

;; check new data in the origin
(get-all-names origin-conn)
;; => #{["Charlie"] ["Dorothy"] ["Alice"] ["Bob"]}


;; check new data in the clone
(get-all-names clone-conn)
;; => #{["Charlie"] ["Alice"] ["Bob"]}

;; reconnect clone
(def clone-conn (d/connect clone-uri))

;; check clone again
(get-all-names clone-conn)
;; => #{["Charlie"] ["Dorothy"] ["Alice"] ["Bob"]}

(defn reconnect []
  (let [state (atom {:chan (chan)
                     :conn (d/connect clone-uri)})]
    (go-loop []
      (let [event (<! (:chan @state))]
        (case event
          :stop (println :stopping)
          :reconnect (do
                       (println :reconnect)
                       (swap! state assoc :conn (d/connect clone-uri))
                       (recur))
          (recur))))
    (go-loop [l (.length (clojure.java.io/file (str clone-dir "/.dat/content.signatures")))]
      (<! (timeout 2000))
      (let [new-l (.length (clojure.java.io/file (str clone-dir "/.dat/content.signatures")))]
        (when-not (= l new-l)
          (>! (:chan @state) :reconnect))
        (recur new-l)))
    state))

(def state (reconnect))

(put! (:chan @state) :reconnect)

;; add new data to origin and check it
(d/transact origin-conn [{:name "Eve"}])

(get-all-names origin-conn)
;; => #{["Charlie"] ["Dorothy"] ["Alice"] ["Eve"] ["Bob"]}

;; you'll see the :reconnect message
(get-all-names (:conn @state))
;; => #{["Charlie"] ["Dorothy"] ["Alice"] ["Eve"] ["Bob"]}

;; let's try again
(d/transact origin-conn [{:name "Freddy"}])
(get-all-names origin-conn)
;; => #{["Charlie"] ["Dorothy"] ["Freddy"] ["Alice"] ["Eve"] ["Bob"]}

(get-all-names (:conn @state))
;; => #{["Charlie"] ["Dorothy"] ["Freddy"] ["Alice"] ["Eve"] ["Bob"]}
