(ns textnancial.fetch
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]))

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

(defn build-job
  [file & {:keys [host port name]
           :or {host "localhost"
                port "27017"}}]
  (do ))

;(defn links [] (lazy-read-csv "D:/data/alllinks_within150.txt"))

;(links)

(def synm {"jan" 1 "feb" 2 "mar" 3 "apr" 4 "jun" 6 "jul" 7 "sep" 9 "oct" 10 "dec" 12 "nov" 11 "aug" 8 "may" 5})

(defn to-maps
  [coll]
  (->> (map #(zipmap [:id :cik :conm :report_type :url :date] %) coll)
       (map #(assoc % :url (:url %)))
       (map #(assoc % :date (->> (re-find #"(\d{2})(\w{3})(\d{4})" (:date %))
                                 rest
                                 ((fn [x] (str (last x) "-"
                                        (get synm (second x)) "-"
                                        (first x)))))))))

;(defn maps [] (to-maps (links)))

;(maps)

#_(defn fetch-text
  [coll]
  (doseq [entry coll]
    (let [oid (ObjectId.)
          con (mg/connect)
          db (mg/get-db con "textnancial")]
      (->> (assoc entry :content (slurp (:url entry)))
           (#(assoc % :_id oid))
           (mmc/insert db "raw_data")))
    ;(Thread/sleep 200)
    ))

;(fetch-text (maps))

;(def data-ref (ref (maps)))

(def counter-ref (ref 27860))

(defn reverse-cons-seq
  [list]
  (for [x (range 1 (+ 1 (count list)))]
    (take x list)))

(defn iter-str
  [list]
  (map #(apply str %) (reverse-cons-seq list)))


(defn log
  [file info]
  (with-open [f-out (io/writer file :append true)]
    (binding [*out* f-out]
      (println (java.util.Date.) info))))


(defn download-item
  [item url-key destination]
  (let [dir (url-key item)
        uri (str destination dir)]
    (when-not (.exists (io/file uri))
      (let [text (slurp (str "http://www.sec.gov/Archives/" dir))
            folds (iter-str (list* destination (re-seq #".+?/" dir)))
            _ (doseq
                [f folds]
                (.mkdir (io/as-file f)))]
        (with-open [f-out (io/writer uri)]
          (binding [*out* f-out]
            (print text)))))))



(defn pull
  [data-ref]
  (dosync
   (when-let
     [[s & ss] @data-ref]
     (ref-set data-ref ss)
     s)))

(defn download-txt
  [_ data-ref]
  (when-let [item (pull data-ref)]
    (do
      (send-off *agent* download-txt data-ref)
      (download-item item))))

(defn main
  [coll ])

;(safe-line-seq "D:/log.txt")

;(def a (->> (filter #(re-find #"reading url" %) (safe-line-seq "D:/log.txt"))
;     (map #(re-find  #"reading url problem happened in (.+)" %))
;     (map second)
;     (map #(assoc {} :url %))))

;(doseq [item a]
;  (download-item item))


