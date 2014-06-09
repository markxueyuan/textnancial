(ns textnancial.scratch
  (:require [net.cgrand.enlive-html :as h]))

(defn get-content
  [url]
  (let [html (h/html-resource (java.net.URL. url))
        content (h/select html [:#contentDiv :#filerDiv :.mailerAddress])]
    content))

;(get-content "http://www.sec.gov/Archives/edgar/data/1750/0001104659-06-047248-index.html")

