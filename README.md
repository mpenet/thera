# Thera

Simple and extensible Clojure DSL to generate Cassandra CQL 2.0 queries.

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
         (columns (as-range :a :b)
                  :reversed true
                  :first 100)

         ;; variant using name columns
         ;; (columns [:a :b "c" 12]
         ;;          :reversed true
         ;;          :first 100)

         (where

         ;; the row key can be provided in different formats or can be a filter on keys
          (pk 1)

          ;; (pk "a")

          ;; KEY IN ...
          ;; (pk [1 2 "a" 4])
          ;; (pk "1" [1 2 "a" 4])

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
          ))))


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

### TODO

* Administrative commands support (CREATE, ALTER, TRUNCATE, DROP)

* JDBC based client

* Schema support for painless encoding/decoding

* Proper documentation/tests.

## License

Copyright (C) 2011 Max Penet

Distributed under the Eclipse Public License, the same as Clojure.
