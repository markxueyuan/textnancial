(ns textnancial.extract
  (:import [java.io File BufferedInputStream FileInputStream]
           [net.htmlparser.jericho Source TextExtractor Renderer])
  (:require [clojure.java.io :as io]))


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
  (let [input (str "E:/" directory)
        folds (iter-str (list* "E:/" "clean/" (re-seq #".+?/" directory)))
        _ (doseq
            [f folds]
            (.mkdir (io/as-file f)))
        output (str "E:/clean/" directory)
        err "does not exist yet!"]
    (try (extract-text input output)
      (catch Throwable e
        (do
          (println directory err)
          (log "E:/log_of_clean.txt" (str directory " " err)))))))

(clean-text "hehe/0001299933-08-000995.txt")
