(ns textnancial.fetch
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [monger.core :as mg]
            [monger.collection :as mc]
            [monger.multi.collection :as mmc];mirror of mc, but with db as first argument for every function.
            [monger.conversion :refer [from-db-object]]
            [textnancial.fetch :as fetch])
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

(defn log
  [file info]
  (with-open [f-out (io/writer file :append true)]
    (binding [*out* f-out]
      (println info))))

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


(defn match-credit-info
  [address]
  (let [doc (str "H:/" address)]
    (if (.exists (io/file doc))
      (->> (safe-line-seq doc)
           (map #(re-find credit-regex %))
           (zipmap (map str (rest (range))))
           (remove #(nil? (val %)))
           (reduce conj {})
           doall)
      (log "E:/missing_log.txt" address))))

(defn match-table-of-contents
  [address]
  (let [doc (str "H:/" address)]
    (when (.exists (io/file doc))
      (->> (safe-line-seq doc)
           (map #(re-find table-regex %))
           (zipmap (map str (rest (range))))
           (remove #(nil? (val %)))
           (reduce conj {})
           doall))))


;(match-credit-info "data/0000950129-07-001091.txt")
;(match-table-of-contents "data/0000950129-07-001091.txt")
(def connection (mg/connect {:host "192.168.1.184" :port 27017}))

(def db (mg/get-db connection "textnancial"))

(defn write-into-mongo
  [collection entry]
  (let [address (:url entry)
        credit (match-credit-info address)
        table (match-table-of-contents address)
        result (assoc entry :credit credit :table table)]
    (mmc/insert db collection result)))

;(write-into-mongo "test" {:url "data/0000950129-07-001091.txt" :haha "hehe"})

(doall (map (partial write-into-mongo "credit_info") (fetch/maps)))
