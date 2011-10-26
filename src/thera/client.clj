(ns thera.client
  (:use [thera schema codec])
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

(defrecord Col [name value])
(defn make-col [name value]
  (Col. name value))

(defn rs-col->clj-col
  [^TypedColumn col schema]
  (let [col-name-str (.getNameString col) ]
    (make-col (decode (jdbc-types->cljk-type (.getNameType col))
                      (column-name-type schema (keyword col-name-str))
                      col-name-str)

              (decode (jdbc-types->cljk-type (.getValueType col))
                      (column-value-type schema)
                      (.getValueString col)))))

(defn rs->clj-row
  [^CResultSet rs schema]
  (make-row (.getRowId rs (.getRow rs))
            (let [ccount (.. rs getMetaData getColumnCount)]
              (if (> ccount 1) ;; id count as 1 row
                (doall
                 (for [index (range 1 ccount)]
                   (-> (.getColumn rs index)
                       (rs-col->clj-col schema)
                       )))
                []))))

(defn resultset->clj
  [^CResultSet resultset schema]
  (loop [rs resultset
         response {:rows [] :meta {} :schema schema}]
    (if (.next rs)
      (recur rs
             (update-in response [:rows]
                        #(conj % (rs->clj-row rs schema))))
      response)))
