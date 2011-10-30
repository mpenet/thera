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

    (select :user
         (limit 100)
         (using  :concistency :QUORUM
                 :timestamp 123123
                 :TTL 123)

         ;; using a column range with optional parameters first and reversed
         (fields (as-range :a :b)
                  :reversed true
                  :first 100)

         ;; variant using name columns
         ;; (fields [:a :b "c" 12]
         ;;          :reversed true
         ;;          :first 100)

         ;; if you dont provide a "fields" fn it will default to *

         ;; count()
         (fields ["count()"])


         (where

         ;; the row key can be provided in different formats or can be a filter on keys
          (pk 1)

          ;; (pk "a")

          ;; KEY IN ...
          ;; (pk [1 2 "a" 4])

          ;; aliased key
          ;; (pk :kalias1 1)
          ;; (pk :kalias1 "a")

          ;; mix of aliased key + <KEY> IN ...
          ;; (pk :kalias1 [1 2 3 4])
          ;; (pk :kalias1 [1 2 "a" 4])

          ;; key range
          ;; (pk {:gt 1 :lte 2})
          ;; (pk {:gt 1})

          ;; key range + alias
          ;; (pk :keyalias1
          ;;     {:gt 1})
          ;; (pk :alias {:gt 1 :lt 2})

          ;; secondary indexes
          (columns [:gt "name" 1]
                   [:eq "pwd" "password"]
                   [:eq "gender" "male"])))))


### INSERT

    (insert :users

            ;; same format and options as in the select query example
            (pk :pk-alias 1)

            ;; optional
            (using  :concistency :QUORUM
                    :timestamp 123123
                    :TTL 123)

            ;; colums to be inserted,
            (values
            {:col1 :test
             :col2 "value2"}))


### UPDATE

    (update :users
            (where (pk :pk-alias 1))
                   (using  :concistency :QUORUM
                           :timestamp 123123
                           :TTL 123)
                   (values
                    {:col1 :value1
                     :col2 :value2}))

### DELETE

    (delete :users
            (columns [:a :b])
            (where (pk :pk-alias 1))
            (using  :concistency :QUORUM))

### Parameterised queries

Positional notation is possible using "?" values, named notation
should be possible using keywords (untested).


## CLIENT

It is still a work in progress, and is in a very rough (low level)
state at the moment.

### Example

    (use 'thera.client)

    (-> (make-datasource {:keyspace "foo"})
        get-connection
        (prepare-statement "SELECT * FROM bar")
        execute
        (decode-result :guess))

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

* :guess -> guesses from schema meta-data if available

* :schema -> ex: (decode-result :schema user-schema)


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

Still a work in progress, only works for decoding at the moment.

    (defschema User

      ;; type for row-key name and value
      :row-key {:types [:string :string]
                ;; key alias if any
                :alias :foo}

      ;; default types for all columns
      :columns {:types [:string :string]
                ;; column with value type different from default
                :exceptions {"date" :integer}})

This will return a schema instance that you can pass to resultset->result.

Supported types:

* `:string`
* `:integer`
* `:long`
* `:float`
* `:double`
* `:json`
* `:clj` ;; storing raw clj code as str, serialization support will come later
* `:uuid`
* `:bytes`

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

* Counters support

* DDL (CREATE, ALTER, TRUNCATE, DROP)

* Proper documentation/tests.

## Notes

Inspired by [korma](https://github.com/ibdknox/Korma), [clojureQL](https://github.com/LauJensen/clojureql) and [clojure-hbase-schemas](https://github.com/compasslabs/clojure-hbase-schemas)

## License

Copyright (C) 2011 Max Penet

Distributed under the Eclipse Public License, the same as Clojure.
