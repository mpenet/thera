(ns thera.codec
  (:require [clj-json.core :as json])
  (:import [org.apache.cassandra.utils FBUtilities]
           ;; soon to become [org.apache.cassandra.utils Hex]
           ))

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



(defmulti decode (fn [type value] type))

(defmethod decode :string [_ value]
  (String. value "UTF-8"))

(defmethod decode :integer [_ value]
  (-> value bytes->hex Integer/parseInt))

(defmethod decode :long [_ value]
  (-> value bytes->hex Long/parseLong))

(defmethod decode :float [_ value]
  (-> value bytes->hex Float/parseFloat))

(defmethod decode :double [_ value]
  (-> value bytes->hex Double/parseDouble))

(defmethod decode :json [_ value]
  (-> value (decode :string) (json/parse-string true)))

(defmethod decode :clj [_ value]
  (-> value (decode :string) read-string))

(defmethod decode :uuid [_ value]
  (->> (decode :string value)
       (partition 4)
       (interleave  [nil nil "-" "-" "-" "-" nil nil])
       (apply concat)
       (apply str)))

(defmethod decode :default [_ value] value)
