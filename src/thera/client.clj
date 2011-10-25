(ns thera.client
  ;; (:use )
  ;; (:require )
  (:import
   [java.sql DriverManager PreparedStatement ResultSet]
   [org.apache.cassandra.cql.jdbc
    CassandraDriver
    CassandraConnection
    CResultSet]))

(def defaults
  {:host "localhost"
   :port 9160
   :keyspace "thera"})

(defn ^CassandraConnection make-connection
  [conf]
  (let [{:keys [host port keyspace]} (merge defaults conf)]
   (DriverManager/getConnection
    (format "jdbc:cassandra://%s:%s/%s" host port keyspace))))

(defn ^PreparedStatement prepare
  [^CassandraConnection connection cql-query]
  (.prepareStatement connection cql-query))

(defn ^CResultSet execute-query
  [^PreparedStatement statement]
  (.executeQuery statement))

;; (defn resultset->map [^CResultSet rs]
;;   (loop [amap {}]
;;     (when (not (.isNull rs)))

;;     )
;;   )

;; (defn execute
;;   [^PreparedStatement statement]
;;   (.exectute statement))
