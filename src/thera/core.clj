(ns thera.core
  "Simple DSL to generate Cassandra CQL queries
   https://github.com/apache/cassandra/blob/trunk/doc/cql/CQL.textile
FIXME: reundancies to get rid of"
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
  `(assoc-in ~q [:query :set] ~(cql/apply-transforms values)))

(defn def-cols [q values]
  (update-in q [:query :defs] merge values))

(defn def-pk [q & values]
  (assoc-in q [:query :defs :pk] values))

(defn with [q values]
  (assoc-in q [:query :with] values))

(defn index-name [q value]
  (assoc-in q [:query :index-name] value))

(defn data-source [q ds]
  (assoc q :data-source ds))

(defn decoder-type [q d]
  (assoc q :decoder-type d))


(defprotocol PQuery
  (as-cql [this]))

(defrecord Query [template query]
  PQuery
  (as-cql [this]
    (cql/make-query template (:query this)))

  clojure.lang.IDeref
  (deref [this]
    (let [generated-q (as-cql this)
          response  (-> (get this :data-source *data-source*)
                        client/get-connection
                        (client/prepare-statement (generated-q 0))
                        (client/bind-parameters (generated-q 1))
                        client/execute)]
      ;; only decode when we have a ResultSet
      (if (client/decodable? response)
        (client/decode-result response (get this :decoder-type *decoder-type*))
        response))))

;; verbs

(defmacro select [cf & steps]
  `(-> (select* ~cf) ~@steps))

(defmacro insert [cf & steps]
  `(-> (insert* ~cf) ~@steps))


(defmacro update [cf & steps]
  `(-> (update* ~cf) ~@steps))

(defmacro delete [cf & steps]
  `(-> (delete* ~cf) ~@steps))

(defmacro truncate [cf & steps]
  `(-> (truncate* ~cf) ~@steps))

(defmacro create-cf [cf & steps]
  `(-> (create-cf* ~cf) ~@steps))

(defmacro create-ks [ks & steps]
  `(-> (create-ks* ~ks) ~@steps))

(defmacro create-index [cf index-name & steps]
  `(-> (create-index* ~cf ~index-name) ~@steps))

(defmacro drop-index [index-name & steps]
  `(-> (drop-index* ~index-name) ~@steps))

(defmacro drop-cf [cf & steps]
  `(-> (drop-cf* ~cf) ~@steps))

(defmacro drop-ks [ks & steps]
  `(-> (drop-ks* ~ks) ~@steps))

(defn select* [cf]
  (Query. ["SELECT" :fields "FROM" :cf :where :using :limit]
          {:cf cf
           :fields [[:*]]}))

(defn insert* [cf]
  (Query. ["INSERT INTO" :cf :values :using]
          {:cf cf}))

(defn update* [cf]
  (Query. ["UPDATE" :cf :using :set :where]
          {:cf cf}))

(defn delete* [cf]
  (Query. ["DELETE" :fields "FROM" :scope :using :where]
          {:scope cf
           :fields [[:*]]}))

(defn truncate* [cf]
  (Query. ["TRUNCATE" :cf]
          {:cf cf}))

(defn create-cf* [cf]
  (Query. ["CREATE COLUMNFAMILY" :cf :defs :with]
          {:cf cf}))

(defn create-ks* [ks]
  (Query. ["CREATE KEYSPACE" :ks :defs :with]
          {:ks ks}))

(defn create-index* [cf col-name]
  (Query. ["CREATE INDEX" :index-name "ON" :cf "(" :col-name ")"]
          {:cf cf :col-name col-name}))

(defn drop-index* [index-name]
  (Query. ["DROP INDEX" :index-name]
          {:index-name index-name}))

(defn drop-cf* [cf]
  (Query. ["DROP COLUMNFAMILY" :cf]
          {:cf cf}))

(defn drop-ks* [ks]
  (Query. ["DROP KEYSPACE" :ks]
          {:ks ks}))