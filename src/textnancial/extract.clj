(ns textnancial.extract
  (:import [java.io File BufferedInputStream FileInputStream]
           [net.htmlparser.jericho Source TextExtractor Renderer])
  (:require [clojure.java.io :as io]
            [textnancial.fetch :refer [maps]]))


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
      (println info))))

(defn extract-text
  [input output]
  (let [source (Source. (BufferedInputStream. (FileInputStream. input)))
        content (Renderer. source)]
    (with-open [f-out (io/writer output)]
      (.writeTo content f-out))))

;(extract-text  "D:/data/0001299933-08-000995.txt" "D:/data/huhu.txt")

(defn clean-text
  [directory]
  (let [input (str "D:/" directory)
        folds (iter-str (list* "D:/" "clean/" (re-seq #".+?/" directory)))
        _ (doseq
            [f folds]
            (.mkdir (io/as-file f)))
        output (str "D:/clean/" directory)
        err "does not exist yet!"]
    (try (extract-text input output)
      (catch Throwable e
        (do
          (println directory err)
          (log "D:/log_of_clean.txt" (str directory " " err)))))))

;(clean-text "hehe/0001299933-08-000995.txt")

#_(doseq [item (maps)]
  (do
    (clean-text (:url item))
    (Thread/sleep 500)))

(clean-text "D:/edgar/data/1750/0001047469-11-006302.txt")
