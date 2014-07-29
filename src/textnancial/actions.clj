(ns textnancial.actions
  (:require [textnancial.fetch :refer [download-item]]
            [monger.collection :as mc]
            [monger.operators :refer [$set]]
            [textnancial.io :refer :all]
            [textnancial.atoms :as a]
            [textnancial.filter :as flt]))

;;;;;;;;;;;;;;;;;;;;plainly download-sec;;;;;;;;;;;;;;;;;;;;

(declare sec-to-file)

(defn plainly-download-sec
  [worker table-name]
  (let [state @worker
        status (:download_status state)
        action (fn [] (send-off worker sec-to-file table-name))]
    (cond (nil? state) (action)
          (= status "done") (action)
          (= status "failed") (action)
          :else (action))))

(defn sec-to-file-help
  [_ table-name url-key destination]
  (when-let [item (find-one-in-mongo table-name {:download_status nil})]
    (let [a (atom nil)
          id (:_id item)
          taken (update-one-in-mongo table-name
                                     {:_id id :download_status nil}
                                     {$set {:download_status "working"}})
          s (fn [] (try (do
                   (download-item item url-key destination)
                   (reset! a "done")
                   @a)
              (catch Exception e
                (do (reset! a "failed")
                  @a))))]
      (if (= taken 1)
        (let [s (s)]
        (do (update-one-in-mongo table-name
                                 {:_id id :download_status "working"}
                                 {$set {:download_status s}})
          (println s)
          (plainly-download-sec *agent* table-name)
          )
        (do
          (println "haha")
          (plainly-download-sec *agent* table-name)
          ))))))


(defn sec-to-file
  [_ table]
  (sec-to-file-help _ table :filename "D:/"))


;;;;;;;;;;;;;;;;;;;;;;match the regex;;;;;;;;;;;;;;;;;;;;;;;;
(declare match-regex)

(defn search-text-single-condition
  [worker table-name]
  (send worker match-regex table-name))

(defn match-regex
  [_ table-name]
    (when-let [item (find-one-in-mongo table-name {:search_status nil})]
    (let [a (atom nil)
          id (:_id item)
          act (fn [] (flt/match-info (a/url-key item) a/location a/regex))
          taken (update-one-in-mongo table-name
                                     {:_id id :search_status nil}
                                     {$set {:search_status "working"}})
          s (fn [] (try (do
                   (update-one-in-mongo table-name
                                        {:_id id :search_status "working"}
                                     {$set {a/result-key (act)}})
                   (reset! a "done")
                   @a)
              (catch Exception e
                (do (reset! a "failed")
                  @a))))]
      (if (= taken 1)
        (let [s (s)]
        (do (update-one-in-mongo table-name
                                 {:_id id :download_status "working"}
                                 {$set {:download_status s}})
          (println s)
          (search-text-single-condition *agent* table-name)
          )
        (do
          (println "haha")
          (search-text-single-condition *agent* table-name)
          ))))))

