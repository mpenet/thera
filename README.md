# Thera

Extensible Cassandra CQL DSL + Client with Schema support.

It is a work in progress, expect bugs, missing features and headaches.

## Goal / Why

Provide a CQL based client for Cassandra, without having to deal with
Thrift history from the java client libraries out there (the Cassandra
maintainers said that removing Thrift as a requirement is a
longer-term goal).

This is also the occasion to have some fun with a DSL, Cassandra
capabilities, and help the Cassandra maintainers finding/fixing bugs
at this early stage of CQL.

It is worth noting that CQL is quite young and doens't support 100% of
cassandra current features, ex: no support for super columns, but
compound columns will be available in the (hopefully near) future and
should be better in this regard, instead of a two-deep structure, you
can have one of arbitrary depth.

## Usage

### SELECT

    ;; simple key lookup
    (select :foo (where (= key "bar")))

    >> "SELECT * FROM foo WHERE key = 'bar'"


    ;; query for list of keys
    (select :foo (where (in keyalias [1 2 "baz" :bar])))))

    >> "SELECT * FROM foo WHERE key in (1, 2, 'baz', bar)"


    ;; range of keys
    (select :foo (where (and (> key 1) (key <= 2))))

    >> "SELECT * FROM foo WHERE key > 1 and key <= 2"


    ;; key + column index
    (select :foo
         (where
          (and
           (= key :foo)
           (> :name 1)
           (= :pwd "password")
           (= :gender "male"))))

    >> "SELECT * FROM foo WHERE key = foo and name > 1 and pwd = 'password' and gender = 'male'"


    ;; field selection
    (select :foo (fields [:bar "baz"]))

    >> "SELECT bar, 'baz' FROM foo"


    ;; field N range
    (select :foo (fields :reversed true
                         :first 100))

    >> "SELECT REVERSED FIRST 100 FROM foo"


    ;; column range
    (select :foo (fields (as-range :a :b)))

    >> "SELECT a...b FROM foo"


    ;; passing additional options (valid for any query type)
    (select :foo (using :consistency :QUORUM
                        :timestamp 123123
                        :TTL 123))

    >> "SELECT * FROM foo USING CONSISTENCY QUORUM and TIMESTAMP 123123 and TTL 123"

and more...

### INSERT

    (insert :foo
             (= key 123)
             (values {:bar "baz"
                      :alpha "beta"})
             (using  :consistency :QUORUM
                     :timestamp 123123
                     :TTL 123))

    >> "INSERT INTO foo (key, bar, alpha) VALUES (123, 'baz', 'beta') USING CONSISTENCY QUORUM and TIMESTAMP 123123 and TTL 123"


### UPDATE

    (update :foo
            (where (= key-alias 1))
            (using  :consistency :QUORUM
                    :timestamp 123123
                    :TTL 123)
            (values
             {:col1 "value1"
              :col2 "value2"}))

    >> "UPDATE foo USING CONSISTENCY QUORUM and TIMESTAMP 123123 and TTL 123 SET col1 = 'value1', col2 = 'value2' WHERE key-alias = 1"


    ;; update/increase with counter + regular columns
    (update :foo
            (where (= pk-alias 1))
            (values
              {:col1 "value1"
               :col2 "value2"
               :col3 (+ 100)}))

    >> "UPDATE foo SET col1 = 'value1', col2 = 'value2', col3 = col3 + 100 WHERE pk-alias = 1"

### DELETE

    (delete :foo
            (fields [:a :b])
            (where (= pk-alias 1))
            (using  :consistency :QUORUM))

    >> "DELETE a, b FROM foo USING CONSISTENCY QUORUM WHERE pk-alias = 1"

### BATCH

    (batch
        (select :foo)
        (insert :foo
                (= key 123)
                (values {:bar "baz"
                         :alpha "beta"})
                (using  :consistency :QUORUM
                        :timestamp 123123
                        :TTL 123))
        (delete :foo
                (fields [:a :b])
                (where (= pk-alias 1))
                (using  :consistency :QUORUM)))

    >> "BATCH BEGIN
            SELECT * FROM foo;INSERT INTO foo (key, bar, alpha) VALUES (123, 'baz', 'beta') USING CONSISTENCY QUORUM and TIMESTAMP 123123 and TTL 123;
            DELETE a, b FROM foo USING CONSISTENCY QUORUM WHERE pk-alias = 1
        APPLY BATCH"

More details about query formats [here](https://github.com/mpenet/thera/blob/master/test/thera/test/query.clj)


## CLIENT

It uses Cassandra Jdbc driver and provides the basic building blocks to
something more idiomatic that will come later.

### Example

    (use 'thera.client)

    (-> (make-datasource {:keyspace "foo"})
        get-connection
        (prepare-statement "SELECT * FROM bar")
        execute
        (decode-result :server-schema))

    {:rows
     [{:id #<UUID 1438fc5c-4ff6-11e0-b97f-0026c650d722>,
       :cols
       ({:name "age", :value 35}
        {:name "birthdate", :value 120976}
        {:name "id", :value #<UUID 1438fc5c-4ff6-11e0-b97f-0026c650d722>}
        {:name "username", :value "mpenet"})}],
     :meta
     #<CResultSetMetaData org.apache.cassandra.cql.jdbc.CResultSet$CResultSetMetaData@1bb5d53a>}

There are 3 decoder availables at the moment:

* :bytes -> returns the raw values/name

* :server-schema -> gets types for keys/values from schema meta-data on the server

* :client-schema -> local schema, see below, ex: (decode-result :schema user-schema)

### Extending the decoders

You can add your own decoder as follows:

    (defmethod decode-result :mydecoder
      [^CResultSet rs _ & args]
      (assoc
        (make-result
           (map-rows rs
                     #(make-row
                       (my-row-key-fn %)
                       (map-columns % as-something-col-fn)))
           (rs-meta rs))
           :another-field ['foo]))


### Schema

It is used for decoding only at the moment, mainly when you dont have
a server-schema defined already (then server-schema mode is better
suited).

In the near future there will be only 1 schema type and syncronisation
with server schema from the client definitions, including other options.

    (defschema User

      ;; type for row-key name and value
      :row-key {:types [:utf-8 :integer]
                ;; key alias if any
                :alias :foo}

      ;; default types for all columns
      :columns {:types [:utf-8 :string]
                ;; column with value type different from default
                :exceptions {"date" :integer}})

This will return a schema instance that you can pass to decode.

Supported types:

`:utf-8` `:ascii` `:boolean` `:integer` `:int32` `:decimal`  `:long` `:float` `:double` `:json` `:clj` `:bytes` `:counter`  `:date`  `:uuid`   `:lexical-uuid`  `:time-uuid`

Defaults to :bytes

### Extending the schema types

    (use 'thera.codec)

    (defmethod decode :csv [_ value]
      (you-csv-decoder-fn (String. value "UTF-8")))

## INSTALLATION

    lein plugin install lein-localrepo "0.3"
    lein localrepo coords cassandra-jdbc/cassandra-jdbc-1.0.5-SNAPSHOT.jar | xargs lein localrepo install
    lein deps

## TODO

* Parameterised query ("?" by default, as-cql fn etc)

* DDL (CREATE, ALTER, TRUNCATE, DROP)

* Higher level api for Schema based use (CRUD and later Schema syncronisation with server)

* Proper documentation/tests.

## Notes

Inspired by [korma](https://github.com/ibdknox/Korma), [clojureQL](https://github.com/LauJensen/clojureql) and [clojure-hbase-schemas](https://github.com/compasslabs/clojure-hbase-schemas)

## License

Copyright (C) 2011 Max Penet

Distributed under the Eclipse Public License, the same as Clojure.
