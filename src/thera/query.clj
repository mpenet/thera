(ns thera.query
  "Simple DSL to generate Cassandra CQL queries
   https://github.com/apache/cassandra/blob/trunk/doc/cql/CQL.textile"
  (:require [thera.cql :as cql]))

(def apply-merge (partial apply merge))

(defn fields [& fields]
  {:fields fields})

(defn as-range [from to]
  {:range [from to]})

(defn using [& args]
  {:using args})

(defn limit [n]
  {:limit n})

(defmacro where [args]
  `{:where ~(cql/apply-transforms args)})

(defmacro values [values]
  `{:values '~values})

(defmacro select [column-family & steps]
  `(cql/make-query
    ["SELECT" :fields "FROM" :column-family :where :using :limit]
    (apply-merge
     {:column-family ~column-family
      :fields [[:*]]}
     (or ~@steps))))

(defmacro insert [column-family pk & steps]
  `(let [steps-map# (apply-merge {} ~@steps)]
    (cql/make-query
      ["INSERT INTO" :column-family :insert-values :using]
      (merge
       {:column-family ~column-family
        :insert-values
        {:row '~pk
         :values (:values steps-map#)}}
       (dissoc steps-map# :values)))))

(defmacro update [column-family & steps]
  `(let [step-map# (apply-merge ~@steps)]
    (cql/make-query
     ["UPDATE" :column-family :using :set :where]
     (apply-merge
      {:column-family ~column-family}
      (dissoc step-map# :values)
      {:set (:values step-map#)}))))

(defmacro delete [column-family & steps]
  `(cql/make-query
    ["DELETE" :fields "FROM" :column-family :using :where]
    (apply-merge
     {:column-family ~column-family}
     ~@steps)))

(defn batch
  [& args]
  (cql/make-query
   ["BATCH BEGIN\n" :using :queries "\nAPPLY BATCH"]
   (reduce
    (fn [acc arg]
      (if (map? arg)
        (let [[k v] (first arg)]
          (assoc acc k v))
        (update-in acc [:queries] #(concat %1 [%2]) arg)))
    {} args)))
