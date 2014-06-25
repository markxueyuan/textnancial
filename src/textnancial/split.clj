(ns textnancial.split
  (:use clj-excel.core)
  (:require [clojure.string :as string]
            [clojure.java.io :as io]
            [clojure-csv.core :as clj-csv]))


(defn get-data
  [file]
  (-> (lazy-workbook (workbook-hssf file))
      (get "content")))

(get-data "D:/data/nan_yang/part1.xls")


(defn split
  [row]
  (let [row (map str row)
        first4 (vec (take 4 row))
        middles (take 6 (drop 4 row))
        subrows (map #(string/split % #"[\r\n]") middles)
        last4 (vec (drop 10 row))
        rows (apply (partial map (partial vector)) subrows)]
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


(write-csv-quoted "D:/data/nan_yang/part3.xls" "D:/data/nan_yang/part3.csv")




(map #(into [7 2 4 6 9 8] %) [[3 5] [8 9]])

(apply (partial map (partial vector)) [[1 2 3] [4 5 6] [7 8 9]])

(into [1 2 3] [4 5] )





