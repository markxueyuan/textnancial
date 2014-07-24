(ns textnancial.actions
  (:require [textnancial.fetch :refer [download-item]]
            [monger.operators :refer [$set]]
            [textnancial.io :refer :all]))

;;;;;;;;;;;;;;;;;;;;plainly download-sec;;;;;;;;;;;;;;;;;;;;
(declare sec-to-file)

(defn plainly-download-sec
  [worker table-name]
  (let [state @worker
        status (:download_status state)
        action (send-off worker sec-to-file table-name)]
    (cond (nil? state) action
          (= status "done") action
          (= status "failed") action
          :else action)))

(defn sec-to-file-help
  [_ table-name url-key destination]
  (when-let [item (find-one-in-mongo table-name {:download_status nil})]
    (let [a (atom nil)
          id (:_id item)
          s (try (do
                   (download-item item url-key destination)
                   (reset! a "done")
                   @a)
              (catch Exception e
                (do (reset! a "failed")
                  @a)))
          taken (update-one-in-mongo table-name
                                     {:_id id :download_status nil}
                                     {$set {:download_status "working"}})]
      (if (= taken 1)
        (do (update-one-in-mongo table-name
                                 {:_id id :download_status "working"}
                                 {$set {:download_status s}})
          (println s)
          (send-off *agent* (fn [_] (assoc item :download_status s)))
          (plainly-download-sec *agent* table-name)
          )
        (do (send-off *agent* (fn [_] nil))
          (plainly-download-sec *agent* table-name)
          (println "haha")
          )))))

(defn sec-to-file
  [_ table]
  (sec-to-file-help _ table :filename "D:/data/"))


;(sec-to-file-help (agent nil) "amendment" :filename "D:/data/")

(doseq [item (lazy-read-mongo-no-timeout "amendment" {:download_status nil})]
    (let [id (:_id item)]
    (try (do
           (download-item item :filename "D:/data/")
           (update-one-in-mongo "amendment"
                                 {:_id id}
                                 {$set {:download_status "done"}}))
      (catch Throwable e (update-one-in-mongo "amendment"
                                 {:_id id}
                                 {$set {:download_status "failed"}})))
    ))


  #_(let [func (fn [item] (let [id (:_id item)]
    (try (do
           (download-item item :filename "D:/data/")
           (update-one-in-mongo "amendment"
                                 {:_id id}
                                 {$set {:download_status "done"}}))
      (catch Throwable e (update-one-in-mongo "amendment"
                                 {:_id id}
                                 {$set {:download_status "failed"}})))))]
    (doall (pmap func (lazy-read-mongo "amendment" {:download_status nil}))))
