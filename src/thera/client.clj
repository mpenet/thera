(ns thera.client
  (:import [java.sql DriverManager PreparedStatement]
           [org.apache.cassandra.cql.jdbc
            CassandraDataSource
            CassandraConnection
            CResultSet
            TypedColumn]))

(Class/forName "org.apache.cassandra.cql.jdbc.CassandraDriver")

(defn ^CassandraDataSource make-datasource
  ([{:keys [host port keyspace]
     :or {host "localhost"
          port 9160
          keyspace "thera"}}]
     (CassandraDataSource. host port keyspace nil nil))
  ([] (make-datasource {})))

(defn ^CassandraConnection get-connection
  [^CassandraDataSource data-source]
  (.getConnection data-source))

(defn ^PreparedStatement prepare-statement
  [^CassandraConnection connection cql-query]
  (.prepareStatement connection cql-query))

(defn execute
  [^PreparedStatement statement]
  (.execute statement)
  (if (= -1 (.getUpdateCount statement))
    (.getResultSet statement)
    statement))


(defrecord Result [meta rows])
(defn make-result [meta rows]
  (Result. meta rows))

(defrecord Row [id cols])
(defn make-row [id cols]
  (Row. id cols))

(defrecord Col [name value])
(defn make-col [name value]
  (Col. name value))

(defn rs-col->clj-col
  [^TypedColumn col]
  (make-col (.. col getNameType (compose (.. col getRawColumn name)))
            (.getValue col)))

(defn rs->clj-row
  [^CResultSet rs]
  (make-row
   (.getObject rs 1)
   (let [ccount (.. rs getMetaData getColumnCount)]
     (if (> ccount 1) ;; id count as 1 row
       (doall
        (for [index (range 1 (inc ccount))]
          (-> (.getColumn rs index)
              rs-col->clj-col)))
       []))))

(defn resultset->result
  [^CResultSet resultset]
  (let [result (-> resultset .getMetaData (make-result []))]
    (assoc result
      :rows (loop [rs resultset
                   rows []]
              (if (.next rs)
                (recur rs  (conj rows (rs->clj-row rs)))
                rows)))))
