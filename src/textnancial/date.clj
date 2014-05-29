(ns textnancial.date
  (:require [clj-time.core :as t]
            [clj-time.format :as f]
            [clj-time.coerce :as joda]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [monger.core :as mg]
            [monger.collection :as mc]
            [monger.multi.collection :as mmc];mirror of mc, but with db as first argument for every function.
            [monger.conversion :refer [from-db-object]]
            [textnancial.fetch :as fetch]
            [clojure.string :as string]
            [clojure.java.io :as io])
  (:import [com.mongodb MongoOptions ServerAddress WriteConcern BasicDBObject BasicDBList]
           org.bson.types.ObjectId
           java.util.ArrayList)
  (:use clj-excel.core))

(def connection (mg/connect))

(def db (mg/get-db connection "textnancial"))

(def month-synom
  {"January" 1
   "February" 2
   "March" 3
   "April" 4
   "May" 5
   "June" 6
   "July" 7
   "August" 8
   "September" 9
   "October" 10
   "November" 11
   "December" 12})

(defn if-date
  [string]
  (let [m (re-find #"[^-]+" string)
        s (set (keys month-synom))]
    (s m)))

(defn clean
  [dates]
  (when-not (empty? dates)
    (filter #(if-date (second %)) dates)))

(defn reformat
  [string]
  (let [parts (string/split string #"-")
        month (get month-synom (first parts))
        day (read-string (second parts))
        year (read-string (last parts))]
    (string/join "-" [year month day])))

(defn min-distance
  [contrast line-n]
  (if (coll? (first contrast))
    (let [c (->> (map first contrast)
                 (filter #(> line-n %)))]
      (if-not (empty? c)
        (->> (map #(- line-n %) c)
             (apply min))
        1000000))
    (let [c (first contrast)]
      (if (> line-n c)
        (- line-n c)
        1000000))))


(defn determine-date
  [contrast dates]
  (when-not (or (empty? contrast) (empty? dates))
    (let [good-dates (clean dates)]
      (when-not (empty? good-dates)
        (let [line-ns (->> good-dates
                          (map first)
                          (map name)
                          (map read-string))
              days (->> good-dates
                       (map second)
                       (map reformat))
              pairs (->> (map (partial min-distance contrast) line-ns)
                         (#(map vector % days)))
              minimum (apply min-key #(first %) pairs)]
          (when-not (= (first minimum) 1000000)
            (second minimum)))))))

;(determine-date [15 :a] [[:12 "August-12-2013"] [:35 "April-4-2025"]])

(defn add-best-date
  [entry]
  (let [dates (:contract-date entry)]
    (let [m1-60 (determine-date (:m1-60 entry) dates)
          m1-70 (determine-date (:m1-70 entry) dates)
          m1-80 (determine-date (:m1-80 entry) dates)
          m2-60 (determine-date (:m2-60 entry) dates)
          m2-70 (determine-date (:m2-70 entry) dates)
          m2-80 (determine-date (:m2-80 entry) dates)
          m1-date (cond m1-60 m1-60 m1-70 m1-70 m1-80 m1-80 :else nil)
          m2-date (cond m2-60 m2-60 m2-70 m2-70 m2-80 m2-80 :else nil)]
      (assoc entry :m1-date m1-date :m2-date m2-date))))


#_(->> (mmc/find-maps db "credit_date_added")
     (map :contract-date)
     (remove nil?)
     (map vals)
     (apply concat)
     (map #(re-find #"[^-]+" %))
     distinct
     (filter (set (keys month-synom))))

#_(->> (mmc/find-maps db "credit_date_added")
     (map :contract-date)
     (map (partial determine-date "abc"))
     doall)

#_(->> (mmc/find-maps db "credit_date_added")
     (map add-best-date)
     (map #(mmc/insert db "credit_exact_dated" %))
     doall
     )

(defn credit-final
  [entry]
  (assoc {}
    :m2_contract_date (:m2-date entry)
    :m2_80 (when-not (empty? (:m2-80 entry)) "Y")
    :m2_70 (when-not (empty? (:m2-70 entry)) "Y")
    :m2_60 (when-not (empty? (:m2-60 entry)) "Y")
    :m1_contract_date (:m1-date entry)
    :m1_80 (when-not (nil? (:m1-80 entry)) "Y")
    :m1_70 (when-not (nil? (:m1-70 entry)) "Y")
    :m1_60 (when-not (nil? (:m1-60 entry)) "Y")
    :url_dir (:url entry)
    :filing_date (:date entry)
    :report_type (:report_type entry)
    :cik (:cik entry)
    :id (:id entry)
    :corp_name (:conm entry)))

#_(->> (map credit-final (mmc/find-maps db "credit_exact_dated"))
     (map #(mmc/insert db "credit_final" %))
     doall)

(defn write-excel
  [collection sheet file]
  (let [func #(map val %)
        cols (map key (first collection))]
    (->> collection
         (map #(func %))
         (#(build-workbook (workbook-xssf) {sheet (into (vector cols) %)}))
         (#(save % file))
         )))

#_(-> (map credit-final (mmc/find-maps db "credit_exact_dated"))
    (write-excel "ALL_RESULTS" "D:/data/textnancial.xlsx")
    doall)



#_(->> (map credit-final (mmc/find-maps db "credit_exact_dated"))
     (map :url_dir))

(defn reverse-cons-seq
  [list]
  (for [x (range 1 (+ 1 (count list)))]
    (take x list)))

(defn iter-str
  [list]
  (map #(apply str %) (reverse-cons-seq list)))

(defn copy-fitted-file
  [entry]
  (when (= "Y" (:m2_80 entry))
    (let [directory (:url_dir entry)
          input (str "D:/" directory)
          folds (iter-str (list* "D:/" "matched/" (re-seq #".+?/" directory)))
          _ (doseq
            [f folds]
            (.mkdir (io/as-file f)))
        output (str "D:/matched/" directory)]
      (io/copy (io/file input) (io/file output)))))


;(copy-fitted-file {:m2_80 "Y" :url_dir "data/big.txt"})

