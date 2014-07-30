(ns textnancial.filter
  (:require [clojure.data.csv :as csv]
            [clojure.string :as string]
            [clojure.java.io :as io])
  (:use textnancial.io))

(defn log
  [file info]
  (with-open [f-out (io/writer file :append true)]
    (binding [*out* f-out]
      (println info))))

(defn match-info
  [uri location reg-exp]
  (let [doc (str location uri)]
    (when (.exists (io/file doc))
      (let [a (atom [])
            func (fn [number-line]
                   (when-let [re (re-find reg-exp (second number-line))]
                     (swap! a conj (conj number-line re))))]
        (doseq [n-l (map vector (range) (safe-line-seq doc))]
          (func n-l))
        @a))))

(defn assure
  [max-distance vov1 vov2]
  (let [n-2 (map first vov2)
        within (fn [n] (some #(and (> % n) (<= % (+ n max-distance))) n-2))]
    (when (and vov1 vov2)
      (filter #(within (first %)) vov1))))


#_(defn match-contract-date
  [address]
  (let [doc (str "H:/" address)]
    (if (.exists (io/file doc))
      (let [l-seq (safe-line-seq doc)]
        (->> (map #(str %1 " " %2) l-seq (rest l-seq))
             (map #(re-find contract-date-regex %))
             (map #(and (not (nil? %)) (string/join "-" (rest %))))
             (zipmap (map str (rest (range))))
             (filter second)
             (reduce conj {})
             doall))
      (log "E:/missing_log.txt" address))))

;(match-contract-date "edgar/data/764622/0000950123-11-041426.txt")



#_(defn add-contract-date
  [entry]
  (if-not (empty? (:m2-80 entry))
    (assoc entry :contract-date (match-contract-date (:url entry)))
    (assoc entry :contract-date nil)))


#_(doseq [a (map add-contract-date (mmc/find-maps db "credit_result"))]
  (mmc/insert db "credit_date_added" a))




