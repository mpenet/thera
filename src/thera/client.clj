(ns thera.client
  (:use [thera.schema])
  (:import
   [java.sql DriverManager PreparedStatement]
   [org.apache.cassandra.cql.jdbc
    CassandraConnection
    CResultSet]))

(def driver-classname "org.apache.cassandra.cql.jdbc.CassandraDriver")

(defn make-jdbc-url
  [host port keyspace]
  (format "jdbc:cassandra://%s:%s/%s" host port keyspace))

(defn ^CassandraConnection make-connection
  [conf]
  (let [{:keys [host port keyspace]
         :or {host "localhost"
              port 9160
              keyspace "thera"}} conf]
  (DriverManager/getConnection
   (make-jdbc-url host port keyspace))))

(defn ^PreparedStatement prepare
  [^CassandraConnection connection cql-query]
  (.prepareStatement connection cql-query))

(defn ^CResultSet execute-query
  [^PreparedStatement statement]
  (.executeQuery statement))

(defn transform-resultset
  [^CResultSet resultset schema]
  {})


;; (defn resultset->map [^CResultSet rs]
;;   (loop [amap {}]
;;     (when (not (.isNull rs)))

;;     )
;;   )

;; (defn execute
;;   [^PreparedStatement statement]
;;   (.exectute statement))
