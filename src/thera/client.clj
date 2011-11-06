(ns thera.client
  (:require [thera.codec :as codec])

  (:use [thera.schema])

  (:import [java.nio HeapByteBuffer]
   [java.sql DriverManager PreparedStatement]
           [org.apache.cassandra.cql.jdbc
            CassandraDataSource CassandraConnection CResultSet
            TypedColumn CResultSet$CResultSetMetaData]))

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

(defn bind-parameters
  [^PreparedStatement statement params]
  (doseq [i (range 0 (count params))]
    (.setObject statement (inc i) (params i)))
  statement)

(defn execute
  [^PreparedStatement statement]
  (.execute statement)
  (if (= -1 (.getUpdateCount statement))
    (.getResultSet statement)
    statement))

(defrecord Result [rows meta])
(defn make-result [rows meta]
  (Result. rows meta))

(defrecord Row [id cols])
(defn make-row [id cols]
  (Row. id cols))

(defrecord Col [name value])
(defn make-col [name value]
  (Col. name value))

(defn ^CResultSet$CResultSetMetaData rs-meta
  [^CResultSet rs]
  (.getMetaData rs))

(defn ^Integer col-count
  [^CResultSet rs]
  (.getColumnCount (rs-meta rs)))

(defn ^TypedColumn typed-col
  [^CResultSet rs ^Integer index]
  (.getColumn rs index))

(defn ^HeapByteBuffer col-value
  [^TypedColumn col]
  (.. col getRawColumn value))

(defn ^HeapByteBuffer col-name
  [^TypedColumn col]
  (.. col getRawColumn name))

(defn ^HeapByteBuffer row-key
  [^CResultSet rs]
  (java.nio.ByteBuffer/wrap (.getKey rs)))

(defn map-rows
  [^CResultSet rs func]
  (loop [rows []]
    (if (.next rs)
      (recur (conj rows (func rs)))
      rows)))

(defn map-columns
  [^CResultSet rs handler & args]
  (let [ccount (col-count rs)]
    (if (> ccount 0)
      (doall
       (for [index (range 1 (inc ccount))]
         (apply handler rs index args)))
      [])))

(defn as-serv-schema-col
  [^CResultSet rs index]
  (let [^TypedColumn col (typed-col rs index)]
    (make-col (.. col getNameType (compose (col-name col)))
              (.getValue col))))

(defn as-bytes-col
  [rs index]
  (let [^TypedColumn col (typed-col rs index)]
    (make-col
     (col-name col)
     (col-value col))))

(defn as-schema-col
  [rs index schema]
  (let [bytes-col (as-bytes-col rs index)
        col-name (codec/decode (column-name-type schema)
                               (:name bytes-col))]
    (assoc bytes-col
      :name col-name
      :value (if-let [value-type (exception schema col-name)]
               (codec/decode value-type
                             (:value bytes-col))
               (codec/decode (column-value-type schema)
                             (:value bytes-col))))))

(defn- key-index
  [rs row-key-bytes-value]
  (let [pk-hex (codec/bytes->hex row-key-bytes-value)]
    (some
     (fn [index]
       (when (= pk-hex
                (-> (typed-col rs index)
                    col-value
                    codec/bytes->hex))
         index))
     (->> (col-count rs) inc (range 1)))))

(defmulti decode-result (fn [rs mode & rest] mode))

(defmethod decode-result :server-schema
  [^CResultSet rs _ & args]
  (let [key-index-m (memoize key-index)]
    (make-result
     (map-rows rs
               #(make-row
                 (^Object .getObject
                          ^CResultSet %
                          ^Integer
                          (key-index-m % (row-key %)))
                 (map-columns % as-serv-schema-col)))
     (rs-meta rs))))

(defmethod decode-result :bytes
  [^CResultSet rs _ & args]
  (make-result
   (map-rows rs
             #(make-row
               (row-key %)
               (map-columns % as-bytes-col)))
   (rs-meta rs)))

(defmethod decode-result :client-schema
  [^CResultSet rs _ & [schema]]
  (let [k-vtype (key-value-type schema)]
    (make-result
     (map-rows rs
               #(make-row
                 (codec/decode k-vtype (row-key %))
                 (map-columns % as-schema-col schema)))
     (rs-meta rs))))

(defmethod decode-result :default
  [^CResultSet rs _ & args]
  (decode-result rs :server-schema args))

(defn decodable? [response]
  (= (type response) CResultSet))