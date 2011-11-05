(defproject thera "0.1.0-SNAPSHOT"
  :description "Thera - Clojure Cassandra library"

  :dependencies [[org.clojure/clojure "1.2.1"]
                 [org.clojure/clojure-contrib "1.2.0"]
                 [clj-json "0.4.3"]
                 [org.apache.cassandra/cassandra-all "1.0.0"]
                 [cassandra-jdbc/cassandra-jdbc "1.0.5-SNAPSHOT"]]

  :warn-on-reflection true

  :dev-dependencies [[lein-localrepo "0.3"]
                     [swank-clojure "1.4.0-SNAPSHOT" :exclusions
                      [org.clojure/clojure org.clojure/clojure-contrib]]
                     [clojure-source "1.2.1"]])
