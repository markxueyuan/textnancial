(ns textnancial.io
  (:use clj-excel.core)
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.string :as string]
            [monger.core :as mg]
            [monger.collection :as mc]
            [monger.operators :refer [$set]]
            [monger.conversion :refer [from-db-object]]
            [monger.cursor :as cur])
  (:import com.mongodb.DBCollection))


(defn lazy-read-excel-head-on
  [file]
  (let [coll (-> (lazy-workbook (workbook-hssf file))
                 first
                 second)
        head (map keyword (first coll))
        rows (rest coll)]
    (map #(zipmap head %) rows)))


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


(defn lazy-read-csv-head-on
  [file]
  (let [coll (lazy-read-csv file)
        head (map keyword (first coll))
        rows (rest coll)]
    (map #(zipmap head %) rows)))

(defmulti lazy-read-file-head-on
  (fn [file-name]
    (->> (re-find #".+\.(.+)" file-name)
         second)))

(defmethod lazy-read-file-head-on "csv"
  [file-name]
  (lazy-read-csv-head-on file-name))

(defmethod lazy-read-file-head-on "xls"
  [file-name]
  (lazy-read-excel-head-on file-name))

(defn insert-mongo-in-batches
  [coll table & {:keys [indexed]}]
  (let [conn (mg/connect)
        db (mg/get-db conn "jobs")]
    (when indexed (mc/ensure-index db table (array-map indexed 1)))
    (mc/insert-batch db table coll)))

(defn insert-mongo-one-by-one
  [coll table & {:keys [indexed]}]
  (let [conn (mg/connect)
        db (mg/get-db conn "jobs")]
    (when indexed (mc/ensure-index db table (array-map indexed 1)))
    (doseq [q coll]
      (mc/insert db table q))))

(defmacro find-one-in-mongo
  [table-name condition]
  `(let [conn# (mg/connect)
        db# (mg/get-db conn# "jobs")]
    (mc/find-one-as-map db# ~table-name ~condition)))

(defmacro update-one-in-mongo
  [table-name condition change]
  `(let [conn# (mg/connect)
        db# (mg/get-db conn# "jobs")]
    (.getN (mc/update db# ~table-name ~condition ~change {:multi false}))))


(defmacro lazy-read-mongo
  [table-name condition]
  `(let [conn# (mg/connect)
        db# (mg/get-db conn# "jobs")]
    (mc/find-maps db# ~table-name ~condition)))


(defmacro lazy-read-mongo-no-timeout
  [table-name condition]
  `(let [conn# (mg/connect)
        db# (mg/get-db conn# "jobs")
        cur# (mc/find db# ~table-name ~condition)]
     (cur/add-options cur# :notimeout)
     (cur/format-as cur# :map)))

