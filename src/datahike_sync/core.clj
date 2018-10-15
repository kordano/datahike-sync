(ns datahike-sync.core
  (:require [datahike.api :as d]
            [juxt.dirwatch :refer [watch-dir]]))


(def master-dir "/tmp/master-dat")
(def master-uri (str "datahike:file://" master-dir))

(comment
  ;; cleanup database if necessary
  (d/delete-database master-uri)

  ;; create a database at this place
  (d/create-database master-uri)
  )

(def master-conn (d/connect master-uri))

(defn get-all-names [conn]
  (d/q '[:find ?n
         :where [?e :name ?n]]
       @conn))

;; lets add some data and wait for the transaction
@(d/transact master-conn [{:db/id 1 :name "Alice" :age 33}
                          {:db/id 2 :name "Bob" :age 37}
                          {:db/id 3 :name "Charlie" :age 55}])

(get-all-names master-conn)
;; => #{["Charlie"] ["Alice"] ["Bob"]}

(def slave-dir "/tmp/slave-dat")
(def slave-uri (str "datahike:file://" slave-dir))

;; create a dat in the master-dir directory and share it. Then clone the dat to the slave-dir directory. This should take a second or two.

;; connect to cloned connection
(def slave-conn (d/connect slave-uri))

;; let's check the cloned data
(get-all-names slave-conn)
;; => #{["Charlie"] ["Alice"] ["Bob"]}


;; add new data to the master database
@(d/transact master-conn [{:db/id 4 :name "Dorothy"}])

;; check new data in the master
(get-all-names master-conn)
;; => #{["Charlie"] ["Dorothy"] ["Alice"] ["Bob"]}


;; check new data in the slave
(get-all-names slave-conn)
;; => #{["Charlie"] ["Alice"] ["Bob"]}

;; reconnect slave
(def slave-conn (d/connect slave-uri))

;; check slave again
(get-all-names slave-conn)
;; => #{["Charlie"] ["Dorothy"] ["Alice"] ["Bob"]}

;; create slave state
(def slave-state (atom {:conn (d/connect slave-uri) :watcher nil}))
(def slave-meta-dir (clojure.java.io/file (str slave-dir "/meta")))

;; add file watcher function
(defn on-meta-change [state file]
  (prn "File changed:" (.getPath (:file file)))
  (swap! state assoc :conn (d/connect slave-uri)))

;; add watcher to file changes
(swap! slave-state assoc :watcher (watch-dir
                                   (partial on-meta-change slave-state)
                                   slave-meta-dir))

;; add new data to master and check it
@(d/transact master-conn [{:db/id 5 :name "Eve"}])

(get-all-names master-conn)
;; => #{["Charlie"] ["Dorothy"] ["Alice"] ["Eve"] ["Bob"]}

;; wait a moment for the data to propagate through the peer to peer network
;; check slave state connection
(get-all-names (:conn @slave-state))
;; => #{["Charlie"] ["Dorothy"] ["Alice"] ["Eve"] ["Bob"]}
