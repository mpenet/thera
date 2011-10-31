(ns thera.codec
  (:require [clj-json.core :as json])
  (:import
   [org.apache.cassandra.cql.jdbc
    JdbcAscii JdbcUTF8 JdbcInteger JdbcInt32 JdbcLong JdbcDouble JdbcFloat
    JdbcBytes JdbcCounterColumn JdbcDecimal JdbcUUID JdbcLexicalUUID
    JdbcTimeUUID JdbcDate JdbcBoolean]
   ;; soon to become [org.apache.cassandra.utils Hex]
   [org.apache.cassandra.utils FBUtilities]))

(defn ^"[B" hex->bytes
  [^String hex]
  (FBUtilities/hexToBytes hex))

(defn ^String bytes->hex
  [^"[B" bytes]
  (FBUtilities/bytesToHex bytes))

(defmulti decode (fn [type value] type))

(defmethod decode :ascii
  [_ value]
  (.. JdbcAscii/instance (compose value)))

(defmethod decode :utf-8
  [_ value]
  (.. JdbcUTF8/instance (compose value)))

(defmethod decode :integer
  [_ value]
  (.. JdbcInteger/instance (compose value)))

(defmethod decode :int32
  [_ value]
  (.. JdbcInt32/instance (compose value)))

(defmethod decode :long
  [_ value]
  (.. JdbcLong/instance (compose value)))

(defmethod decode :float
  [_ value]
  (.. JdbcFloat/instance (compose value)))

(defmethod decode :double
  [_ value]
  (.. JdbcDouble/instance (compose value)))

(defmethod decode :bytes
  [_ value]
  (.. JdbcBytes/instance (compose value)))

(defmethod decode :counter
  [_ value]
  (.. JdbcCounterColumn/instance (compose value)))

(defmethod decode :decimal
  [_ value]
  (.. JdbcDecimal/instance (compose value)))

(defmethod decode :uuid
  [_ value]
  (.. JdbcUUID/instance (compose value)))

(defmethod decode :lexical-uuid
  [_ value]
  (.. JdbcLexicalUUID/instance (compose value)))

(defmethod decode :time-uuid
  [_ value]
  (.. JdbcTimeUUID/instance (compose value)))

(defmethod decode :date
  [_ value]
  (.. JdbcDate/instance (compose value)))

(defmethod decode :bool
  [_ value]
  (.. JdbcBoolean/instance (compose value)))

(defmethod decode :json
  [_ value]
  (-> value (decode :utf-8) (json/parse-string true)))

(defmethod decode :clj
  [_ value]
  (-> value (decode :utf-8) read-string))


;; (defmulti encode (fn [type value] type))

;; (defmethod encode :string
;;   [_ value]
;;   (-> (.getBytes value) bytes->hex))

;; (defmethod encode :integer
;;   [_ value]
;;   (-> value FBUtilities/toByteArray bytes->hex))

;; (defmethod encode :long
;;   [_ value]
;;   (-> value FBUtilities/toByteArray bytes->hex))

;; (defmethod encode :json
;;   [_ value]
;;   (-> value json/generate-string (encode :string)))

;; (defmethod encode :clj
;;   [_ value]
;;   (-> value prn (encode :string)))

;; (defmethod encode :uuid
;;   [_ value]
;;   (encode :string value))

;; (defmethod encode :default [_ value] value)
