(ns textnancial.scratch
  (:require [net.cgrand.enlive-html :as h]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [monger.core :as mg]
            [monger.collection :as mc]
            [monger.multi.collection :as mmc];mirror of mc, but with db as first argument for every function.
            [monger.conversion :refer [from-db-object]])
  (:import [com.mongodb MongoOptions ServerAddress WriteConcern BasicDBObject BasicDBList]
           org.bson.types.ObjectId
           java.util.ArrayList))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;preparing jobs;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

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

(defn links [] (lazy-read-csv "D:/data/cik-gvkey.csv"))

;(links)

(def synm {"jan" 1 "feb" 2 "mar" 3 "apr" 4 "jun" 6 "jul" 7 "sep" 9 "oct" 10 "dec" 12 "nov" 11 "aug" 8 "may" 5})

(defn to-maps
  [coll]
  (let [col-name (map keyword (first coll))
        rows (rest coll)
        fdate #(->> (re-find #"(\d{2})(\w{3})(\d{4})" %)
                    rest
                    ((fn [x] (str (last x) "-"
                                        (get synm (second x)) "-"
                                        (first x)))))]
    (->> (map #(zipmap col-name %) rows)
         (map #(assoc % :datereport (fdate (:datereport %)) :datefiled (fdate (:datefiled %)))))))

(defn maps [] (to-maps (links)))

;(maps)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;scratch data;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn log
  [file info]
  (with-open [f-out (io/writer file :append true)]
    (binding [*out* f-out]
      (println info))))


(defn get-content
  [entry log-file]
  (let [url (str (:websiteSEC entry) "-index.html")
        html (h/html-resource (java.net.URL. url))
        content (h/select html [:#contentDiv])]
    (try (assoc entry :file-text content)
      (catch Throwable e
        (do
          (println "Scratching problem happend in" url)
          (log log-file url))))))

;(get-content (first (maps)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;write to db;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def connection (mg/connect {:host "192.168.1.184" :port 27017}))

(def db (mg/get-db connection "textnancial"))

(defn main
  [coll collection log-file]
  (doseq [entry coll]
    (let [content (get-content entry log-file)]
      (do
        (when-not (nil? content)
          (mmc/insert db collection content))
        (Thread/sleep 100)))))

;(main (maps) "location")





