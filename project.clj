(defproject thera "1.0.0-SNAPSHOT"
  :description "Thera - Clojure Cassandra library"

  :dependencies [[org.clojure/clojure "1.2.1"]
                 [org.clojure/clojure-contrib "1.2.0"]
                 [org.apache.cassandra/cassandra-all "1.0.0"]]

  :dev-dependencies [[swank-clojure "1.4.0-SNAPSHOT" :exclusions
                      [org.clojure/clojure org.clojure/clojure-contrib]]
                     [clojure-source "1.2.1"]])