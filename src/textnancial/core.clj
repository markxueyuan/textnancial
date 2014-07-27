(ns textnancial.core
  (:use textnancial.io
        textnancial.actions)
  (:require [clojure.string :as string]
            [textnancial.fetch :as fetch]))

(defn upload-job
  [file-name table-name & {:keys [indexed]}]
  (let [coll (lazy-read-file-head-on file-name)
        parts (partition-all 500 coll)]
     (doseq [q parts]
       (insert-mongo-in-batches q table-name :indexed indexed))))

(defn run-job
  [table-name job-dispatcher worker-num]
  (let [workers (set (repeatedly worker-num #(agent nil
                                                    :error-mode :continue
                                                    :error-handler (fn [the-agent exception]
                                                                     (fetch/log "D:/data/agent_log.txt"
                                                                                (.getMessage exception))))))]
    (doseq [w workers]
      (job-dispatcher w table-name))))

(defn recovery
  [table-name job-status target]
  (update-multiple-in-mongo table-name {job-status target} {monger.operators/$set {job-status nil}}))

(defn failure-recovery
  [table-name job-status]
  (recovery table-name job-status "failed"))

(defn clear-interrupted
  [table-name job-status]
  (recovery table-name job-status "working"))

;(run-job "amendment" plainly-download-sec 5)

;(upload-job "D:/data/allSEClinks.csv" "amendment" :indexed :filename)

;(failure-recovery "amendment" :download_status)

;(clear-interrupted "amendment" :download_status)


;(update-multiple-in-mongo "amendment"  {} {monger.operators/$set {:download_status nil}})
