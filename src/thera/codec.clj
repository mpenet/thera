(ns thera.codec
  ^{:doc "Encoding and decoding utilities."}
  (import [org.apache.cassandra.cql.jdbc]))

(defmulti encode (fn [from-type to-type value] [from-type to-type]))

(defmethod encode [:string :text] [_ _ value])
(defmethod encode [:number :int] [_ _ value])
(defmethod encode [:clj :test] [_ _ value])
(defmethod encode [:json :ascii] [_ _ value])

;; etc ...

;; (defmethod encode [:number :bigint] [_ value])
;; (defmethod encode [:bytes :blob] [_ value])
;; (defmethod encode [:boolean :boolean] [_ value])

;; (defmethod encode [:number :counter] [_ value])
;; (defmethod encode [:decimal] [_ value])
;; (defmethod encode [:double] [_ value])
;; (defmethod encode [:float] [_ value])
;; (defmethod encode [:int] [_ value])
;; (defmethod encode :text [_ value])
;; (defmethod encode :timestamp [_ value])
;; (defmethod encode :uuid [_ value])
;; (defmethod encode :varchar [_ value])
;; (defmethod encode :varint [_ value])


(defmulti decode (fn [from-type to-type value] [(class from-type) to-type]))



(defmethod decode [:bytes Object] [_ _ value]
  value)

(defmethod decode [:bytes Object] [_ _ value]
  value)

(defmethod decode :default [_ _ value]
  (println value)

  value)

;; (defmethod decode :ascii [_ _ value])
;; (defmethod decode :bigint [_ _ value])
;; (defmethod decode :blob [_ _ value])
;; (defmethod decode :boolean [_ _ value])
;; (defmethod decode :counter [_ _ value])
;; (defmethod decode :decimal [_ _ value])
;; (defmethod decode :double [_ _ value])
;; (defmethod decode :float [_ _ value])
;; (defmethod decode :int [_ _ value])
;; (defmethod decode :text [_ _ value])
;; (defmethod decode :timestamp [_ _ value])
;; (defmethod decode :uuid [_ _ value])
;; (defmethod decode :varchar [_ _ value])
;; (defmethod decode :varint [_ _ value])

;; decode from bytes/named-jdtype to clojure type

;; (decode :utf8 :string value)
;; (decode :utf8 :json value)
;; (encode :json value)

(def cljk->jdbc-types
  {:ascii        org.apache.cassandra.cql.jdbc.JdbcAscii
   :bool         org.apache.cassandra.cql.jdbc.JdbcBoolean
   :bytes        org.apache.cassandra.cql.jdbc.JdbcBytes
   :counter      org.apache.cassandra.cql.jdbc.JdbcCounterColumn
   :date         org.apache.cassandra.cql.jdbc.JdbcDate
   :decimal      org.apache.cassandra.cql.jdbc.JdbcDecimal
   :double       org.apache.cassandra.cql.jdbc.JdbcDouble
   :float        org.apache.cassandra.cql.jdbc.JdbcFloat
   :int32        org.apache.cassandra.cql.jdbc.JdbcInt32
   :integer      org.apache.cassandra.cql.jdbc.JdbcInteger
   :lexical-uuid org.apache.cassandra.cql.jdbc.JdbcLexicalUUID
   :long-type    org.apache.cassandra.cql.jdbc.JdbcLong
   :time-uuid    org.apache.cassandra.cql.jdbc.JdbcTimeUUID
   :utf8         org.apache.cassandra.cql.jdbc.JdbcUTF8
   :uuid         org.apache.cassandra.cql.jdbc.JdbcUUID})

(def jdbc-types->cljk-type
  (apply array-map (interleave (vals cljk->jdbc-types)
                               (keys cljk->jdbc-types))))
