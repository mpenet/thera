(ns thera.client
  (:require [thera.codec :as codec])
  (:use [thera.schema])
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

(defn col-count
  [^CResultSet rs]
  (.. rs getMetaData getColumnCount))

(defn col-bytes-value
  [^TypedColumn col]
  (.. col getRawColumn getValue))

(defn map-columns
  [^CResultSet rs handler & args]
  (let [ccount (col-count rs)]
    (if (> ccount 0)
      (doall
       (for [index (range 1 (inc ccount))]
         (apply handler rs index args)))
      [])))

(defn as-guess-col
  [^CResultSet rs index]
  (let [^TypedColumn col (.getColumn rs index)]
    (make-col (.. col getNameType (compose (.. col getRawColumn name)))
              (.getValue col))))

(defn as-bytes-col
  [rs index]
  (let [^TypedColumn col (.getColumn rs index)]
    (make-col
     (.. col getNameString getBytes)
     (col-bytes-value col))))

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

(defn key-index
  "FIXME: that s quite an ugly way to do it .., but i dont see another way for now, it s 'guess' mode only"
  [rs row-key-bytes-value]
  (let [pk-hex (codec/bytes->hex row-key-bytes-value)]
    (some
     (fn [index]
       (when (= pk-hex
                (-> (.getColumn rs index)
                    col-bytes-value
                    codec/bytes->hex))
         index))
     (->> (col-count rs) inc (range 1)))))

(defmulti decode-row (fn [mode & rest] mode))

(defmethod decode-row :guess [_ ^CResultSet rs & args]
  (make-row
   (.getObject rs (key-index rs (.getKey rs)))
   (map-columns rs as-guess-col)))

(defmethod decode-row :bytes [_ ^CResultSet rs & args]
  (make-row
   (.getKey rs)
   (map-columns rs as-bytes-col)))

(defmethod decode-row :schema [_ ^CResultSet rs & args]
  (let [schema (-> args first :as)]
    (make-row
     (codec/decode (key-value-type schema) (.getKey rs))
     (map-columns rs as-schema-col schema))))

(defmethod decode-row :default [_ ^CResultSet rs & args]
  (apply decode-row :guess rs args))

(defn resultset->result
  [^CResultSet resultset & {:keys [decoder] :as args}]
  (let [result (-> resultset .getMetaData (make-result []))
        row-decoder (partial decode-row decoder)]
    (assoc result
      :rows (loop [rs resultset
                   rows []]
              (if (.next rs)
                (recur rs  (conj rows (row-decoder rs args)))
                rows)))))