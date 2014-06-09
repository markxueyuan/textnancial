(defproject textnancial "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/data.csv "0.1.2"]
                 [lein-light-nrepl "0.0.10"]
                 [com.novemberain/monger "1.7.0"]
                 [net.htmlparser.jericho/jericho-html "3.1"]
                 [clj-time "0.6.0"]
                 [clj-excel "0.0.1"]
                 [enlive "1.1.5"]]
  :repl-options {:nrepl-middleware [lighttable.nrepl.handler/lighttable-ops]}
  :jvm-opts ["-Xmx1g"])
