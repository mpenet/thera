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

(defprotocol PRowCodec
  (decode-row [this schema])
  (decode-row-id [this schema])
  (decode-cols [this schema])
  )

(defrecord Row [id cols]
  PRowCodec
  (decode-row [this schema]
    (assoc this
        :id (decode-row-id this schema)
        :rows (decode-cols this schema)))

  (decode-row-id [this schema]
    (println schema)
    (println this)


    )
  (decode-cols [this schema])
  )

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
  [^CResultSet rs schema]
  (make-row (.getRowId rs (.getRow rs))
            (let [ccount (.. rs getMetaData getColumnCount)]
              (if (> ccount 1) ;; id count as 1 row
                (doall
                 (for [index (range 1 ccount)]
                   (-> (.getColumn rs index)
                       rs-col->clj-col)))
                []))))

(defn resultset->clj
  [^CResultSet resultset schema]
  (loop [rs resultset
         response {:rows [] :meta {}}]
    (if (.next rs)
      (recur rs (update-in response [:rows]
                  #(conj % (rs->clj-row rs schema))))
      response)))
