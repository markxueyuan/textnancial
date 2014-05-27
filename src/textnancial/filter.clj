(ns textnancial.fetch
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [monger.core :as mg]
            [monger.collection :as mc]
            [monger.multi.collection :as mmc];mirror of mc, but with db as first argument for every function.
            [monger.conversion :refer [from-db-object]]
            [clojure.string :refer (upper-case)])
  (:import [com.mongodb MongoOptions ServerAddress WriteConcern BasicDBObject BasicDBList]
           org.bson.types.ObjectId
           java.util.ArrayList))

(defn lazy-read-csv
  [csv-file]
  (let [in-file (io/reader csv-file)
        csv-seq (csv/read-csv in-file)
        lazy (fn lazy [wrapped]
               (lazy-seq
                (if-let [s (seq wrapped)]
                  (cons (first s) (lazy (rest s)))
                  (.close in-file))))]
    (lazy csv-seq)))

(defn safe-line-seq
  "Similar to line-seq, add a .close at the end."
  [file]
  (let [in-file (io/reader file)
        lazy (fn lazy [wrapped]
             (lazy-seq
              (if-let [line (.readLine wrapped)]
                (cons line (lazy-seq (lazy wrapped)))
                (.close in-file))))]
    (lazy in-file)))

(def credit-regex
  #"(?x)
  CREDIT\sAGREEMENT|
  LOAN\sAGREEMENT|
  CREDIT\sFACILITY|
  LOAN\sAND\sSECURITY\sAGREEMENT|
  LOAN\s&\sSECURITY\sAGREEMENT|
  REVOLVING\sCREDIT|
  FINANCING\sAND\sSECURITY\sAGREEMENT|
  FINANCING\s&\sSECURITY\sAGREEMENT|
  CREDIT\sAND\sGUARANTEE\sAGREEMENT|
  CREDIT\s&\sGUARANTEE\sAGREEMENT
  ")

(def table-regex #"TABLE OF CONTENTS")

(remove nil? (seq (doall (for [line (safe-line-seq "D:/data/0000950129-07-001091.txt")]
  (re-find credit-regex line)))))


