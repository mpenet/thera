(ns thera.cql
  ^{:doc "Query maps to CQL transformations"}
  (:use [clojure.string :only [upper-case join]])
  (:require [clojure.walk :as walk]))

(def ^{:dynamic true} *params*)

(defn set-param! [value]
  (swap! *params* conj value)
  "?")

;; CQL special functions and operators

(defmacro definfix [& ops]
  `(do
     ~@(doall
        (for [op# ops]
          `(defn ~(-> op# (str "*") symbol) [& args#]
             (interpose ~(keyword op#) args#))))))

(definfix and or = - + < > <= >= in)

(def predicates
  {'- 'thera.cql/-*
   '+ 'thera.cql/+*
   '= 'thera.cql/=*
   '> 'thera.cql/>*
   '< 'thera.cql/<*
   '<= 'thera.cql/<=*
   '>= 'thera.cql/>=*
   'and 'thera.cql/and*
   'or 'thera.cql/or*
   'in 'thera.cql/in*
   'key :key})

(def apply-transforms (partial walk/prewalk-replace predicates))

(def fns {:count-fn "count()"})

;; string manip helpers

(def join-and (partial join " and "))
(def join-spaced (partial join " "))
(def join-coma (partial join ", "))
(def format-eq (partial format "%s = %s"))

(defn flatten-seq
  "Same as flatten, but ignores vectors"
  [x]
  (filter (complement seq?)
          (rest (tree-seq seq? seq x))))

(defprotocol PEncoder
  (encode [value]))

(extend-protocol PEncoder

  clojure.lang.Symbol
  (encode [value] (str value))

  clojure.lang.Keyword
  (encode [value]
    (get fns value (name value)))

  clojure.lang.Sequential
  (encode [value]
    (format "(%s)" (join-coma (map encode value))))

  clojure.lang.IPersistentMap
  (encode [value]
    (->> value
         (map (fn [[k v]]
                (format-eq (encode k) (encode v))))
         join-coma))

  java.lang.Object
  (encode [value]
    (set-param! value)))

;; CQL Query translation

(defmulti emit (fn [token value] token))

(defmethod emit :column-family
  [_ column-family]
  (encode column-family))

(defmethod emit :fields
  [_ args]
  (let [[columns opts] (if (or (vector? (first args))
                               (map? (first args)))
                         [(first args) (rest args)]
                         [nil args])]
    (->> [(when (seq opts) (emit :fields-options (apply array-map opts)))
          (when (seq columns) (emit :fields-value columns))]
         (filter identity)
         join-spaced)))

(defmethod emit :fields-value
  [_ value]
  (if (map? value)
    (let [range (:range value)]
      (format "%s...%s"
              (-> range first encode)
              (-> range second encode)))
    (join-coma (map encode value))))

(defmethod emit :fields-options
  [_ opts]
  (join-spaced (filter identity (map #(apply emit %) opts))))

(defmethod emit :first
  [_ first]
  (str "FIRST " first))

(defmethod emit :reversed
  [_ reversed]
  "REVERSED")

(defmethod emit :using
  [_ args]
  (str "USING "
       (join-and
        (for [[n value] (partition 2 args)]
          (str (-> n name upper-case)
               " " (encode value))))))

(defmethod emit :where
  [_ where]
  (->> where
       flatten-seq
       (map encode)
       (cons "WHERE")
       join-spaced))

(defmethod emit :values
  [_ values]
  (format "%s VALUES %s"
         (-> values keys encode)
         (-> values vals encode)))

(defmethod emit :set
  [_ values]
  (str "SET "
       (->> (map (fn [[k v]]
                   (if (seq? v) ;; counter
                     (emit :counter [k v])
                     (format-eq (encode k) (encode v))))
                 values)
            join-coma)))

(defmethod emit :counter
  [_ [field-name [op value]]]
  (format-eq (encode field-name)
             (join-spaced [(encode field-name)
                           op (encode value)])))

(defmethod emit  :limit
  [_ limit]
  (str "LIMIT " limit))

(defn make-query
  [query-map]
  (binding [*params* (atom [])]
    [(->> (map (fn [token]
                 (if (string? token)
                   token
                   (when-let [value (token query-map)]
                     (emit token value))))
               (:template query-map))
          (filter identity)
          join-spaced)
     @*params*]))
