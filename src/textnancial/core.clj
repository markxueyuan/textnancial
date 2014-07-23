(ns textnancial.core
  (:require [clojure.string :as string]))

(defmulti upload-job
  (fn [file-name & {:keys [indexed]}]
    (->> (re-find #".+\.(.+)" file-name)
         second)))

(defmethod upload-job "csv"
  [file-name & {:keys [indexed]}]

  )
