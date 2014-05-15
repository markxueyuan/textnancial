(ns textnancial.fetch
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [monger.core :as mg]
            [monger.collection :as mc]
            [monger.multi.collection :as mmc];mirror of mc, but with db as first argument for every function.
            [monger.conversion :refer [from-db-object]])
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

(defn links [] (lazy-read-csv "D:/data/alllinks.txt"))

(def synm {"jan" 1 "feb" 2 "mar" 3 "apr" 4 "jun" 6 "jul" 7 "sep" 9 "oct" 10 "dec" 12 "nov" 11 "aug" 8 "may" 5})

(defn to-maps
  [coll]
  (->> (map #(zipmap [:cik :conm :report_type :url :date] %) coll)
       (map #(assoc % :url (str "http://www.sec.gov/Archives/" (:url %))))
       (map #(assoc % :date (->> (re-find #"(\d{2})(\w{3})(\d{4})" (:date %))
                                 rest
                                 ((fn [x] (str (last x) "-"
                                        (get synm (second x)) "-"
                                        (first x)))))))))

(defn maps [] (to-maps (links)))



(defn fetch-text
  [coll]
  (doseq [entry coll]
    (let [oid (ObjectId.)
          con (mg/connect)
          db (mg/get-db con "textnancial")]
      (->> (assoc entry :content (slurp (:url entry)))
           (#(assoc % :_id oid))
           (mmc/insert db "raw_data")))
    ;(Thread/sleep 200)
    ))

(fetch-text (maps))


