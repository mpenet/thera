(ns thera.codec
  ^{:doc "Encoding and decoding utilities."})

(comment

  Should be able to encode multiple clj types to cassandra types, ex keyword as text, or ascii,
  java.lang.number to various numeric types etc...

  ascii	ASCII character string
  bigint	8-byte long
  blob	Arbitrary bytes (no validation)
  boolean	true or false
  counter	Counter column, (8-byte long)
  decimal	Variable-precision decimal
  double	8-byte floating point
  float	4-byte floating point
  int	4-byte int
  text	UTF8 encoded string
  timestamp	Date + Time, encoded as 8 bytes since epoch
  uuid	Type 1, or type 4 UUID
  varchar	UTF8 encoded string
  varint	Arbitrary-precision integer
  )

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


(defmulti decode (fn [from-type to-type value] [from-type to-type]))

(defmethod decode :ascii [_ _ value])
(defmethod decode :bigint [_ _ value])
(defmethod decode :blob [_ _ value])
(defmethod decode :boolean [_ _ value])
(defmethod decode :counter [_ _ value])
(defmethod decode :decimal [_ _ value])
(defmethod decode :double [_ _ value])
(defmethod decode :float [_ _ value])
(defmethod decode :int [_ _ value])
(defmethod decode :text [_ _ value])
(defmethod decode :timestamp [_ _ value])
(defmethod decode :uuid [_ _ value])
(defmethod decode :varchar [_ _ value])
(defmethod decode :varint [_ _ value])