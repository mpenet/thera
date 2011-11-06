(ns thera.query
  "Simple DSL to generate Cassandra CQL queries
   https://github.com/apache/cassandra/blob/trunk/doc/cql/CQL.textile"
  ;; (:refer-clojure :exclude [set])
  (:require [thera.cql :as cql]))

(def make cql/make-query)

;; verbs

(defmacro select [cf & steps]
  `(-> (select* ~cf) ~@steps make))

(defn select*
  [cf]
  {:template ["SELECT" :fields "FROM" :column-family :where :using :limit]
   :column-family cf
   :fields [[:*]]})

(defmacro insert [cf & steps]
  `(-> (insert* ~cf) ~@steps make))

(defn insert*
  [cf]
  {:template ["INSERT INTO" :column-family :values :using]
   :column-family cf})

(defmacro update [cf & steps]
  `(-> (update* ~cf) ~@steps make))

(defn update*
  [cf]
  {:template ["UPDATE" :column-family :using :set :where]
   :column-family cf})

(defmacro delete [cf & steps]
  `(-> (delete* ~cf) ~@steps make))

(defn delete*
  [cf]
  {:template ["DELETE" :fields "FROM" :column-family :using :where]
   :column-family cf
   :fields [[:*]]})


;; clauses

(defn fields [q & fields]
  (assoc q :fields fields))

(defn as-range [from to]
  {:range [from to]})

(defn using [q & args]
  (assoc q :using args))

(defn limit [q n]
  (assoc q :limit n))

(defn pk [q n]
  (assoc q :pk n))

(defmacro where [q args]
  `(assoc ~q :where ~(cql/apply-transforms args)))

(defn values [q values]
  (assoc q :values values))

(defmacro set [q values]
  `(assoc ~q :set '~values))
