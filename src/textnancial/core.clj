(ns textnancial.core
  (:use textnancial.io)
  (:require [clojure.string :as string]))

(defmacro template
  [s]
  `(let [coll# ~s
         parts# (partition-all 500 coll#)]
     (doseq [q# parts#]
       (insert-mongo-in-batches q# ~'table-name :indexed ~'indexed))))

(defmulti upload-job
  (fn [file-name table-name & {:keys [indexed]}]
    (->> (re-find #".+\.(.+)" file-name)
         second)))

(defmethod upload-job "csv"
  [file-name table-name & {:keys [indexed]}]
  (template (lazy-read-csv-head-on file-name)))

(defmethod upload-job "xls"
  [file-name table-name & {:keys [indexed]}]
  (template (lazy-read-excel-head-on file-name)))
