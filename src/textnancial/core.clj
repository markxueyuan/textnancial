(ns textnancial.core
  (:use textnancial.io
        textnancial.actions)
  (:require [clojure.string :as string]))

(defn upload-job
  [file-name table-name & {:keys [indexed]}]
  (let [coll (lazy-read-file-head-on file-name)
        parts (partition-all 500 coll)]
     (doseq [q parts]
       (insert-mongo-in-batches q table-name :indexed indexed))))

(defn run-job
  [table-name job-dispatcher worker-num]
  (let [workers (set (repeatedly worker-num #(agent nil)))]
    (doseq [w workers]
      (job-dispatcher w table-name))))

(run-job "amendment" plainly-download-sec 5000)

;(upload-job "D:/data/allSEClinks.csv" "amendment" :indexed :filename)
