(ns textnancial.regex)

(def credit-regex
  #"(?x)
  CREDIT\sAGREEMENT|
  LOAN\sAGREEMENT|
  CREDIT\sFACILITY|
  LOAN\sAND\sSECURITY\sAGREEMENT|
  LOAN\s&\sSECURITY\sAGREEMENT|
  REVOLVING\sCREDIT|
  FINANCING\sAND\sSECURITY\sAGREEMENT|
  FINANCING\s&\sSECURITY\sAGREEMENT|
  CREDIT\sAND\sGUARANTEE\sAGREEMENT|
  CREDIT\s&\sGUARANTEE\sAGREEMENT
  ")

(def table-regex #"TABLE OF CONTENTS")

(def contract-date-regex #"(?:D|d)ated as of (\D+?)(?:(?:&nbsp;)|\s)(\d+?), (\d{4})")

(def violation-regex #"")
