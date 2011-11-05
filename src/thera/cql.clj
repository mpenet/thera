(ns thera.cql
  ^{:doc "Query maps to CQL transformations"}
  (:use [clojure.string :only [upper-case join]])
  (:require [clojure.walk :as walk]))

(def ^{:dynamic true} *params*)

(defn set-param! [value]
  (swap! *params* conj value)
  "?")

;; CQL special functions and operators

(defmacro definfix [op]
  `(defn ~(-> op (str "*") symbol) [& args#]
     (interpose ~(keyword op) args#)))

(defmacro definfixn [& ops]
  `(do ~@(doall (for [op# ops]
                  `(definfix ~op#)))))

(definfixn and or = - + < > <= >= in)

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

(defn realize-pred-form
  [form]
  (->> form
       (walk/prewalk-replace predicates)
       eval))

(def fns {:count-fn "count()"})

;; string manip helpers

(def join-and (partial join " and "))
(def join-spaced (partial join " "))
(def join-coma (partial join ", "))

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
                (format "%s = %s"
                        (encode k)
                        (encode v))))
         join-coma))

  java.lang.Object
  (encode [value]
    (set-param! value)))

;; CQL Query translation

(defmulti translate (fn [token value] token))

(defmethod translate :column-family
  [_ column-family]
  (encode column-family))

(defmethod translate :fields
  [_ args]
  (let [[columns opts] (if (or (vector? (first args))
                               (map? (first args)))
                         [(first args) (rest args)]
                         [nil args])]
    (->> [(when (seq opts) (translate :fields-options (apply array-map opts)))
          (when (seq columns) (translate :fields-value columns))]
         (filter identity)
         join-spaced)))

(defmethod translate :fields-value
  [_ value]
  (if (map? value)
    (let [range (:range value)]
      (format "%s...%s"
              (-> range first encode)
              (-> range second encode)))
    (join-coma (map encode value))))

(defmethod translate :fields-options
  [_ opts]
  (join-spaced (filter identity (map #(apply translate %) opts))))

(defmethod translate :first [_ first]
  (when first (str "FIRST " first)))

(defmethod translate :reversed
  [_ reversed]
  (when reversed "REVERSED"))

(defmethod translate :using
  [_ args]
  (str "USING "
       (join-and
        (for [[n value] (partition 2 args)]
          (str (-> n name upper-case)
               " " (encode value))))))

(defmethod translate :where
  [_ [where]]
  (->> (realize-pred-form where)
       flatten-seq
       (map encode)
       (cons "WHERE")
       join-spaced))

(defmethod translate :insert-values [_ {:keys [row values]}]
  (format"%s VALUES %s"
         (->> values keys (cons (second row)) encode)
         (->> values vals (cons (last row)) encode)))

(defmethod translate :set [_ values]
  (str "SET "
       (->> (map (fn [[k v]]
                   (if (seq? v) ;; counter
                     (translate :counter [k v])
                     (format "%s = %s"
                             (encode k)
                             (encode v))))
                 values)
            join-coma)))

(defmethod translate :counter [_ [field-name [op value]]]
  (format "%s = %s %s %s"
          (encode field-name)
          (encode field-name)
          op
          (encode value)))

(defmethod translate  :limit
  [_ limit]
  (str "LIMIT " limit))

(defmethod translate :queries [_ queries]
  (join ";" queries))

(defn make-query
  [template query]
  (binding [*params* (atom [])]
    [(->> (map (fn [token]
                 (if (string? token)
                   token
                   (when-let [value (token query)]
                     (translate token value))))
               template)
          (filter identity)
          join-spaced)
     @*params*]))
