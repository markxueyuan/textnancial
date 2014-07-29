(ns textnancial.atoms
  (:use [textnancial.regex]))

(def a-location (atom "http://www.sec.gov/Archives/"))

(def location @a-location)

(def a-regex (atom credit-regex))

(def regex @a-regex)

(def a-url-key (atom :filename))

(def url-key @a-url-key)

(def a-result-key (atom :result))

(def result-key @a-result-key)
