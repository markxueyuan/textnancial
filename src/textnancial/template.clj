(ns textnancial.template
  (:require [net.cgrand.enlive-html :as h]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [monger.core :as mg]
            [monger.collection :as mc]
            [monger.multi.collection :as mmc];mirror of mc, but with db as first argument for every function.
            [monger.conversion :refer [from-db-object]]
            [clojure.string :as string])
  (:import [com.mongodb MongoOptions ServerAddress WriteConcern BasicDBObject BasicDBList]
           org.bson.types.ObjectId
           java.util.ArrayList))

(def connection (mg/connect {:host "192.168.1.184" :port 27017}))

(def db (mg/get-db connection "textnancial"))


(defn business-address
  [html]
  (->> (h/select html [[:.mailer (h/nth-of-type 2)] :.mailerAddress])
       (map h/text)
       (map string/trim)))

(def test (first (:file-text (mmc/find-one-as-map db "location_all" {:cikSEC "278166"}))))

(business-address test)

(->> (mmc/find-maps db "location_all")
     (map :file-text)
     (map first)
     (map business-address)
     (map city))

(defn street
  [address]
  (->> (take (- (count address) 2) address)
       (string/join " ")))

(defn city
  [address]
(first (take-last 2 address)))

