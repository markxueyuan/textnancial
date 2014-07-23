(ns textnancial.io
  (:use clj-excel.core)
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.string :as string]
            [monger.core :as mg]
            [monger.collection :as mc]))


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

(defn insert-mongo-in-batches
  [coll table & {:keys [indexed]}]
  (let [conn (mg/connect)
        db (mg/get-db conn "jobs")]
    (when indexed (mc/ensure-index db table (array-map indexed 1)))
    (mc/insert-batch db table coll)))
