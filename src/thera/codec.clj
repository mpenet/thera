(ns thera.codec
  (:require [clj-json.core :as json])
  (:import
   [java.nio HeapByteBuffer]
   [org.apache.cassandra.cql.jdbc
    JdbcAscii JdbcUTF8 JdbcInteger JdbcInt32 JdbcLong JdbcDouble JdbcFloat
    JdbcBytes JdbcCounterColumn JdbcDecimal JdbcUUID JdbcLexicalUUID
    JdbcTimeUUID JdbcDate JdbcBoolean]
   [org.apache.cassandra.utils ByteBufferUtil]))

(defn hex->bytes
  [hex]
  (ByteBufferUtil/hexToBytes hex))

(defn bytes->hex
  [bytes]
  (ByteBufferUtil/bytesToHex bytes))

(defmulti decode (fn [type ^HeapByteBuffer value] type))

(defmethod decode :ascii
  [_ ^HeapByteBuffer value]
  (.. JdbcAscii/instance (compose value)))

(defmethod decode :utf-8
  [_ ^HeapByteBuffer value]
  (.. JdbcUTF8/instance (compose value)))

(defmethod decode :integer
  [_ ^HeapByteBuffer value]
  (.. JdbcInteger/instance (compose value)))

(defmethod decode :int32
  [_ ^HeapByteBuffer value]
  (.. JdbcInt32/instance (compose value)))

(defmethod decode :long
  [_ ^HeapByteBuffer value]
  (.. JdbcLong/instance (compose value)))

(defmethod decode :float
  [_ ^HeapByteBuffer value]
  (.. JdbcFloat/instance (compose value)))

(defmethod decode :double
  [_ ^HeapByteBuffer value]
  (.. JdbcDouble/instance (compose value)))

(defmethod decode :bytes
  [_ ^HeapByteBuffer value]
  (.. JdbcBytes/instance (compose value)))

(defmethod decode :counter
  [_ ^HeapByteBuffer value]
  (.. JdbcCounterColumn/instance (compose value)))

(defmethod decode :decimal
  [_ ^HeapByteBuffer value]
  (.. JdbcDecimal/instance (compose value)))

(defmethod decode :uuid
  [_ ^HeapByteBuffer value]
  (.. JdbcUUID/instance (compose value)))

(defmethod decode :lexical-uuid
  [_ ^HeapByteBuffer value]
  (.. JdbcLexicalUUID/instance (compose value)))

(defmethod decode :time-uuid
  [_ ^HeapByteBuffer value]
  (.. JdbcTimeUUID/instance (compose value)))

(defmethod decode :date
  [_ ^HeapByteBuffer value]
  (.. JdbcDate/instance (compose value)))

(defmethod decode :bool
  [_ ^HeapByteBuffer value]
  (.. JdbcBoolean/instance (compose value)))

(defmethod decode :json
  [_ ^HeapByteBuffer value]
  (-> value (decode :utf-8) (json/parse-string true)))

(defmethod decode :clj
  [_ ^HeapByteBuffer value]
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
