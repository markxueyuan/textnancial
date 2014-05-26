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

(defn links [] (lazy-read-csv "D:/data/alllinks_within150.txt"))

(links)

(def synm {"jan" 1 "feb" 2 "mar" 3 "apr" 4 "jun" 6 "jul" 7 "sep" 9 "oct" 10 "dec" 12 "nov" 11 "aug" 8 "may" 5})

(defn to-maps
  [coll]
  (->> (map #(zipmap [:id :cik :conm :report_type :url :date] %) coll)
       (map #(assoc % :url (:url %)))
       (map #(assoc % :date (->> (re-find #"(\d{2})(\w{3})(\d{4})" (:date %))
                                 rest
                                 ((fn [x] (str (last x) "-"
                                        (get synm (second x)) "-"
                                        (first x)))))))))

(defn maps [] (to-maps (links)))

(maps)

#_(defn fetch-text
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

;(fetch-text (maps))

(def data-ref (ref (maps)))

(def counter-ref (ref 14270))

(defn reverse-cons-seq
  [list]
  (for [x (range 1 (+ 1 (count list)))]
    (take x list)))

(defn iter-str
  [list]
  (map #(apply str %) (reverse-cons-seq list)))


(defn log
  [file info]
  (with-open [f-out (io/writer file :append true)]
    (binding [*out* f-out]
      (println info))))


(defn download-item
  [item]
  (let [dir (:url item)
        text (try (slurp (str "http://www.sec.gov/Archives/" dir))
               (catch Throwable e
                 (do
                   (println "reading url problem happened in " dir)
                   (log "D:/log.txt" (str "reading url problem happened in " dir))
                   )))
        folds (iter-str (list* "D:/" (re-seq #".+?/" dir)))
        file (re-find  #".+/(.+)" dir)
        _ (doseq
            [f folds]
            (.mkdir (io/as-file f)))]
    (do
      (with-open [f-out (io/writer (str "D:/" dir))]
        (binding [*out* f-out]
          (try (print text)
            (catch Throwable e
              (do
                (println "writing file problem happend in dir")
                (log "D:/log.txt" (str "writing file problem happend in " dir)))))))
      (dosync (alter counter-ref inc))
      (let [counts @counter-ref]
        (when (= 0 (mod counts 10))
          (println counts)
          (log "D:/log.txt" counts)))
      (Thread/sleep 500)
    )))

(doseq [item (drop 14270 (maps))]
  (download-item item))


(defn pull
  [data-ref]
  (dosync
   (when-let
     [[s & ss] @data-ref]
     (ref-set data-ref ss)
     s)))

(defn download-txt
  [_ data-ref]
  (when-let [item (pull data-ref)]
    (do
      (send-off *agent* download-txt data-ref)
      (download-item item))))

(defn main
  [coll ])



