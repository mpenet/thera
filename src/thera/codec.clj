(ns thera.codec
  (:require [clj-json.core :as json])
  (:import [java.nio HeapByteBuffer]
           [java.util UUID Date]
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

(def wrap-quotes (partial format "'%s'"))


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



(defmulti encode (fn [type value] type))

(defmethod encode :ascii
  [_ ^String value]
  (wrap-quotes (.. JdbcAscii/instance (toString value))))

(defmethod encode :utf-8
  [_ ^String value]
  (wrap-quotes (.. JdbcUTF8/instance (toString value))))

(defmethod encode :integer
  [_ ^Integer value]
  (.. JdbcInteger/instance (toString value)))

(defmethod encode :int32
  [_ ^Integer value]
  (.. JdbcInt32/instance (toString value)))

(defmethod encode :long
  [_ ^Long value]
  (.. JdbcLong/instance (toString value)))

(defmethod encode :float
  [_ ^Float value]
  (.. JdbcFloat/instance (toString value)))

(defmethod encode :double
  [_ ^Double value]
  (.. JdbcDouble/instance (toString value)))

(defmethod encode :bytes
  [_ ^"[B" value]
  (wrap-quotes (.. JdbcBytes/instance (toString value))))

(defmethod encode :counter
  [_ ^Long value]
  (.. JdbcCounterColumn/instance (toString value)))

(defmethod encode :decimal
  [_ ^BigDecimal value]
  (.. JdbcDecimal/instance (toString value)))

(defmethod encode :uuid
  [_ ^UUID value]
  (.. JdbcUUID/instance (toString value)))

(defmethod encode :lexical-uuid
  [_ ^UUID value]
  (.. JdbcLexicalUUID/instance (toString value)))

(defmethod encode :time-uuid
  [_ ^UUID value]
  (.. JdbcTimeUUID/instance (toString value)))

(defmethod encode :date
  [_ ^Date value]
  (.. JdbcDate/instance (toString value)))

(defmethod encode :bool
  [_ ^Boolean value]
  (.. JdbcBoolean/instance (toString  value)))

(defmethod encode :json
  [_ value]
  (-> value json/generate-string wrap-quotes))

(defmethod encode :clj
  [_  value]
  (-> value prn wrap-quotes))
