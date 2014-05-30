(ns textnancial.fetch
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [monger.core :as mg]
            [monger.collection :as mc]
            [monger.multi.collection :as mmc];mirror of mc, but with db as first argument for every function.
            [monger.conversion :refer [from-db-object]]
            [textnancial.fetch :as fetch]
            [clojure.string :as string])
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

(def contract-date-regex #"(?:D|d)ated as of (\D+?)(?:(?:&nbsp;)|\s)(\d+?), (\d{4})")

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

;(doall (map (partial write-into-mongo "credit_info") (fetch/maps)))

(defn ignore-nil
  [func value]
  (if (nil? value)
    identity
    func))

(defn assure
  [max-dist entry]
  (let [t-o-c (->> (:table entry)
                   keys
                   (map #((ignore-nil name %) %))
                   (map #((ignore-nil read-string %) %)))
        line-n (->> (:credit entry)
                    keys
                    (map #((ignore-nil name %) %))
                    (map #((ignore-nil read-string %) %)))
        words (->> (:credit entry)
                   vals)
        within (fn [n] (some #(and (> % n) (<= % (+ n max-dist))) t-o-c))
        pairs (map vector line-n words)
        clean (fn [coll] (remove #(nil? (first %)) coll))
        within-pairs (filter #(within (first %)) (clean pairs))
        sorted (sort #(< (first %1) (first %2)) (clean pairs))
        top (first sorted)
        within-top (when ((ignore-nil within (first top)) (first top))
                     top)]
    [within-top within-pairs]))

;(map (partial assure 60) (mmc/find-maps db "credit_info"))




(defn add-result
  [entry]
  (let [in-60 (assure 60 entry)
        in-70 (assure 70 entry)
        in-80 (assure 80 entry)
        m1-60 (first in-60)
        m2-60 (second in-60)
        m1-70 (first in-70)
        m2-70 (second in-70)
        m1-80 (first in-80)
        m2-80 (second in-80)]
    (assoc entry :m1-60 m1-60 :m1-70 m1-70 :m2-60 m2-60 :m2-70 m2-70 :m1-80 m1-80 :m2-80 m2-80)))

;(doall (map #(mmc/insert db "credit_result" %) (map add-result (mmc/find-maps db "credit_info_test2"))))

(defn match-contract-date
  [address]
  (let [doc (str "H:/" address)]
    (if (.exists (io/file doc))
      (let [l-seq (safe-line-seq doc)]
        (->> (map #(str %1 " " %2) l-seq (rest l-seq))
             (map #(re-find contract-date-regex %))
             (map #(and (not (nil? %)) (string/join "-" (rest %))))
             (zipmap (map str (rest (range))))
             (filter second)
             (reduce conj {})
             doall))
      (log "E:/missing_log.txt" address))))

;(match-contract-date "edgar/data/764622/0000950123-11-041426.txt")



(defn add-contract-date
  [entry]
  (if-not (empty? (:m2-80 entry))
    (assoc entry :contract-date (match-contract-date (:url entry)))
    (assoc entry :contract-date nil)))


#_(doseq [a (map add-contract-date (mmc/find-maps db "credit_result"))]
  (mmc/insert db "credit_date_added" a))



