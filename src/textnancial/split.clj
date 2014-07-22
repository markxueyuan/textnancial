(ns textnancial.split
  (:use clj-excel.core)
  (:require [clojure.string :as string]
            [clojure.java.io :as io]
            [clojure-csv.core :as clj-csv]))


(defn get-data
  [file]
  (-> (lazy-workbook (workbook-hssf file))
      (get "content")))


(defn split
  [row]
  (let [row (map str row)
        first4 (vec (take 5 row))
        middles (take 4 (drop 5 row))
        subrows (map #(string/split % #"[\r\n]") middles)
        last4 (vec (drop 9 row))
        rows (apply (partial map (partial vector)) subrows)];It is Nice!
    (->> (map #(into first4 %) rows)
         (map #(into % last4))
         )
    ))

(defn write-csv-quoted
  [in-file out-file]
  (let [coll (get-data in-file)
        keys-vec (->> (map #(string/split % #"\n") (vec (first coll)))
                      (map #(apply str %)))
        vals-vecs (mapcat split (rest coll))]
    (with-open [out (io/writer out-file)]
      (binding [*out* out]
        (print (clj-csv/write-csv (vector keys-vec) :force-quote true))
        (doseq [v vals-vecs]
          (print (clj-csv/write-csv (vector v) :force-quote true))
          )))))


(write-csv-quoted "D:/data/amendment.xls" "D:/data/amendment_split2.csv")






