(ns textnancial.template
  (:require [net.cgrand.enlive-html :as h]
            [clojure.java.io :as io]
            [monger.core :as mg]
            [monger.collection :as mc]
            [monger.multi.collection :as mmc];mirror of mc, but with db as first argument for every function.
            [monger.conversion :refer [from-db-object]]
            [clojure.string :as string]
            [clojure.java.io :as io]
            [clojure-csv.core :as csv])
  (:import [com.mongodb MongoOptions ServerAddress WriteConcern BasicDBObject BasicDBList]
           org.bson.types.ObjectId
           java.util.ArrayList)
  (:use clj-excel.core))

(def connection (mg/connect {:host "192.168.1.184" :port 27017}))

;(def connection (mg/connect))

(def db (mg/get-db connection "textnancial"))


(defn business-address
  [html]
  (->> (h/select html [[:.mailer (h/nth-of-type 2)] :.mailerAddress])
       (map h/text)
       (map string/trim)))

(def test1 (first (:file-text (mmc/find-one-as-map db "location_all" {:cikSEC "278166"}))))

;(info test1)


(defn street
  [address]
  (->> (take (- (count address) 2) address)
       (string/join " ")))

(defn location
  [address]
  (-> (first (take-last 2 address))
      (string/split #"\s+")))

(defn city
  [address]
  (let [loc (location address)]
    (->> (take (- (count loc) 2) loc)
         (string/join " "))))

(defn state
  [address]
  (first (take-last 2 (location address))))

(defn zip
  [address]
  (last (location address)))

(defn business-phone
  [address]
  (last address))

(defn ident-info
  [html]
  (->> (h/select html [:.identInfo :> h/any-node])
       (map h/text)
       ;(map string/trim)
       identity
       ))

(defn irs-no
  [ident]
  (nth ident 2))

(defn state-of-incop
  [ident]
  (nth ident 4))


(defn fiscal-year-end
  [ident]
  (nth ident 6))

(defn act
  [ident]
  (nth ident 11))

(defn file-no
  [ident]
  (nth ident 13))

(defn film-no
  [ident]
  (nth ident 15))

(defn sic
  [ident]
  (nth ident 19))

(defn industry
  [ident]
  (nth ident 20))

(defn assistant-director
  [ident]
  (read-string (re-find #"\d" (nth ident 22))))

(defn info
  [html]
  (let [b-a (business-address html)
        i-i (ident-info html)]
    {:street (street b-a)
     :city (city b-a)
     :state (state b-a)
     :zip (zip b-a)
     :business_phone (business-phone b-a)
     :irs_no (irs-no i-i)
     :state_of_incop (state-of-incop i-i)
     :fiscal_year_end (fiscal-year-end i-i)
     :act (act i-i)
     :file_no (file-no i-i)
     :film_no (film-no i-i)
     :sic (sic i-i)
     :industry (industry i-i)
     :assistant_director (assistant-director i-i)}))

(defn infos
  [coll]
  (let [add-on (->> coll
                    (map :file-text)
                    (map first)
                    (map info))]
    (->> coll
         (map #(dissoc % :file-text))
         (map #(into % add-on)))))


(defn new-ident-info
  [html]
  (->> (h/select html [:.identInfo])
       (map h/text)
       ;(map string/trim)
       first))

(defn new-state-of-incop
  [ident]
  (second (re-find #"State of Incorp.: (\w+)" ident)))

(defn add-on
  [html]
  (let [b-a (business-address html)
        i-i (new-ident-info html)]
    {:street (street b-a)
     :city (city b-a)
     :state (state b-a)
     :zip (zip b-a)
     :business_phone (business-phone b-a)
     :state_of_incop (new-state-of-incop i-i)}))

(defn new-info
  [entry]
  (let [add-on (try (add-on (:file-text entry))
                 (catch Throwable e
                   (println (:websiteSEC entry))))
        origin (dissoc entry :file-text)]
    (into origin add-on)))

(defn new-infos
  [coll]
  (map new-info coll))


#_(doseq [info (new-infos (mmc/find-maps db "location_all"))]
    (mmc/insert db "info" info))



(defn write-excel
  [collection sheet file]
  (let [cols (map key (first collection))
        fval #((apply juxt cols) %)]
    (->> collection
         (map fval)
         (#(build-workbook (workbook-xssf) {sheet (map vec (cons cols %))}))
         (#(save % file))
         )))

(defn write-csv
  [coll file]
  (let [keys-vec (keys (first coll))
        vals-vecs (map (apply juxt keys-vec) coll)]
    (with-open [out (io/writer file)]
      (binding [*out* out]
        (print (csv/write-csv (vector (map name keys-vec)) :force-quote true))
        (doseq [v vals-vecs]
          (print (csv/write-csv (vector v) :force-quote true)))))))

;(write-csv (mmc/find-maps db "info"{} {:_id 0}) "D:/data/info.csv")


