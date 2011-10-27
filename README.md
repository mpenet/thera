# Thera

Simple and extensible Clojure DSL to generate Cassandra CQL 2.0 queries.
Includes a basic client.

At the moment it supports CRUD operations including the BATCH command.
It is a work in progress, expect changes, bugs and improvements (see TODO).

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

Schema support has been droped for now since the type of the values
can be infered from the configured types (defaults to bytebuffers if
it s not the case), and the server converts cql query values to the
approriate type from the configured values.

See [CQL doc](https://github.com/apache/cassandra/blob/trunk/doc/cql/CQL.textile)

    Term specification

    Terms are used in statements to specify things such as keyspaces, column families, indexes, column names and values, and keyword arguments. The rules governing term specification are as follows:

    Any single quoted string literal (example: 'apple').
    Unquoted alpha-numeric strings that begin with a letter (example: carrot).
    Unquoted numeric literals (example: 100).
    UUID strings in hyphen-delimited hex notation (example: 1438fc5c-4ff6-11e0-b97f-0026c650d722).
    Terms which do not conform to these rules result in an exception.

    How column name/value terms are interpreted is determined by the configured type.

### Example

    (use 'thera.client)

    (-> (make-datasource {:keyspace "foo"})
        get-connection
        (prepare "SELECT * FROM bar")
        execute-query
        resultset->clj)

    {:rows
     [{:id #<UUID 1438fc5c-4ff6-11e0-b97f-0026c650d722>,
       :cols
       ({:name "age", :value 35}
        {:name "birthdate", :value 120976}
        {:name "id", :value #<UUID 1438fc5c-4ff6-11e0-b97f-0026c650d722>}
        {:name "username", :value "mpenet"})}],
     :meta
     #<CResultSetMetaData org.apache.cassandra.cql.jdbc.CResultSet$CResultSetMetaData@1bb5d53a>}

## INSTALLATION

    lein plugin install lein-localrepo "0.3"
    lein localrepo coords cassandra-jdbc/cassandra-jdbc-1.0.5-SNAPSHOT.jar | xargs lein localrepo install
    lein deps

## TODO

* Add CQL Function (count()), wildcards and counters support.

* Administrative commands support (CREATE, ALTER, TRUNCATE, DROP)

* Proper documentation/tests.

## Notes

Inspired by [korma](https://github.com/ibdknox/Korma) & [clojureQL](https://github.com/LauJensen/clojureql)

## License

Copyright (C) 2011 Max Penet

Distributed under the Eclipse Public License, the same as Clojure.
