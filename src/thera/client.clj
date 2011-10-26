(ns thera.client
  (:use [thera.schema])
  (:import
   [java.sql DriverManager PreparedStatement]
   [org.apache.cassandra.cql.jdbc
    CassandraDataSource
    CassandraConnection
    CResultSet
    TypedColumn]))

(Class/forName "org.apache.cassandra.cql.jdbc.CassandraDriver")

(defn ^CassandraDataSource make-datasource
  [{:keys [host port keyspace]
         :or {host "localhost"
              port 9160
              keyspace "thera"}}]
  (CassandraDataSource. host port keyspace nil nil))

(defn ^CassandraConnection get-connection
  [^CassandraDataSource data-source]
  (.getConnection data-source))

(defn ^PreparedStatement prepare
  [^CassandraConnection connection cql-query]
  (.prepareStatement connection cql-query))

(defn ^CResultSet execute-query
  [^PreparedStatement statement]
  (.executeQuery statement))

(defrecord Row [id cols])
(defn make-row [id cols]
  (Row. id cols))

(defrecord Col [name-type name value-type value])
(defn make-col [name-type name value-type value]
  (Col. name-type name value-type value))

(defn rs-col->clj-col
  [^TypedColumn col]
  (make-col (.getNameType col)
            (.getNameString col)
            (.getValueType col)
            (.getValue col)))

(defn rs->clj-row
  [^CResultSet rs]
  (let [ccount (.. rs getMetaData getColumnCount)]
    (make-row (.getRowId rs (.getRow rs))
              (if (> ccount 0)
                (map (fn [index]
                       (-> (.getColumn rs index)
                           rs-col->clj-col))
                     (range 1 ccount))
                []))))

(defn resultset->clj
  [^CResultSet resultset schema]
  (loop [rs resultset
         response {:rows [] :meta {}}]
    (if (.next rs)
      (update-in response [:rows]
                 #(conj % (rs->clj-row rs)))
      response)))0

;; (defn execute
;;   [^PreparedStatement statement]
;;   (.exectute statement))
