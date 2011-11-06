(ns thera.core
  "Simple DSL to generate Cassandra CQL queries
   https://github.com/apache/cassandra/blob/trunk/doc/cql/CQL.textile"
  (:refer-clojure :exclude [set])
  (:require [thera.cql :as cql]
            [thera.client :as client]))

(def ^{:dynamic true} *data-source* (client/make-datasource {}))
(def ^{:dynamic true} *decoder-type* :server-schema)

(defn set-data-source!
  [data-source]
  (alter-var-root #'*data-source*
                  (constantly data-source)
                  (when (thread-bound? #'*data-source*)
                    (set! *data-source* data-source))))

(defn set-decoder-type!
  [decoder-type]
  (alter-var-root #'*decoder-type*
                  (constantly decoder-type)
                  (when (thread-bound? #'*decoder-type*)
                    (set! *decoder-type* decoder-type))))

(defmacro with-data-source [ds & body]
  `(binding [*data-source* ~ds]
     ~@body))

(defmacro with-decoder-type [d-type & body]
  `(binding [*decoder-type* ~d-type]
     ~@body))

;; clauses

(defn fields [q & fields]
  (assoc-in q [:query :fields] fields))

(defn as-range [from to]
  {:range [from to]})

(defn using [q & args]
  (assoc-in q [:query :using] args))

(defn limit [q n]
  (assoc-in q [:query :limit] n))

(defmacro where [q args]
  `(assoc-in ~q [:query :where] ~(cql/apply-transforms args)))

(defn values [q values]
  (assoc-in q [:query :values] values))

(defmacro set [q values]
  `(assoc-in ~q [:query :set] '~values))

(defn data-source [q ds]
  (assoc q :data-source ds))

(defn decoder-type [q d]
  (assoc q :decoder-type d))


(defprotocol PQuery
  (as-cql [this]))

(defrecord Query [template query data-source decoder-type]

  PQuery
  (as-cql [this]
    (cql/make-query template (:query this)))

  clojure.lang.IDeref
  (deref [this]
    (let [generated-q (as-cql this)]
      (-> data-source
          client/get-connection
          (client/prepare-statement (generated-q 0))
          (client/bind-parameters (generated-q 1))
          client/execute
          (client/decode-result decoder-type)))))

;; verbs

(defmacro select [cf & steps]
  `(-> (select* ~cf) ~@steps))

(defmacro insert [cf & steps]
  `(-> (insert* ~cf) ~@steps))


(defmacro update [cf & steps]
  `(-> (update* ~cf) ~@steps))

(defmacro delete [cf & steps]
  `(-> (delete* ~cf) ~@steps))

(defn select*
  [cf]
  (Query. ["SELECT" :fields "FROM" :column-family :where :using :limit]
          {:column-family cf
           :fields [[:*]]}
          *data-source*
          *decoder-type*))

(defn insert* [cf]
  (Query. ["INSERT INTO" :column-family :values :using]
          {:column-family cf}
          *data-source*
          *decoder-type*))

(defn update* [cf]
  (Query. ["UPDATE" :column-family :using :set :where]
          {:column-family cf}
          *data-source*
          *decoder-type*))

(defn delete* [cf]
  (Query. ["DELETE" :fields "FROM" :column-family :using :where]
          {:column-family cf
           :fields [[:*]]}
          *data-source*
          *decoder-type*))