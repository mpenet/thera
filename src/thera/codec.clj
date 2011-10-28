(ns thera.codec
  (:import ; [org.apache.cassandra.utils ByteBufferUtil]
          ;; [org.apache.cassandra.utils Hex]
           [org.apache.cassandra.utils FBUtilities]))

(def ^{:doc "We need a type object for extend-protocol"}
  byte-array-type
  (Class/forName "[B"))

;; C* api is about to change regarding hex/bytes codecs just use local fns for now
(defn ^String bytes->hex
  [^byte-array-type bytes]
  (FBUtilities/bytesToHex bytes))

(defn ^byte-array-type hex->bytes
  [^String hex]
  (FBUtilities/hexToBytes hex))
