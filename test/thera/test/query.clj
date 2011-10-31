(ns thera.test.query
  (:use [clojure.test]
        [thera.query]))

(deftest select-query
  (is (= "SELECT * FROM foo"
         (select :foo)))

  (is (= "SELECT bar, baz FROM foo"
         (select :foo
              (fields [:bar "baz"]))))

  (is (= "SELECT REVERSED FIRST 100 FROM foo"
         (select :foo
                 (fields :reversed true
                         :first 100))))

  (is (= "SELECT REVERSED FIRST 100 bar, baz FROM foo"
         (select :foo
                 (fields [:bar "baz"]
                         :reversed true
                         :first 100))))

  (is (= "SELECT count() FROM foo"
         (select :foo (fields ["count()"]))))

  (is (= "SELECT a...b FROM foo"
         (select :foo (fields (as-range :a :b))))))

(deftest using-query
  (is (= "SELECT * FROM foo USING CONCISTENCY QUORUM AND TIMESTAMP 123123 AND TTL 123"
         (select :foo (using :concistency :QUORUM
                             :timestamp 123123
                             :TTL 123)))))

(deftest pks
  (is (= "SELECT * FROM foo WHERE KEY = 1"
         (select :foo (where (pk 1)))))

  (is (= "SELECT * FROM foo WHERE KEY = 'bar'"
         (select :foo (where (pk "bar")))))

  (is (= "SELECT * FROM foo WHERE KEY IN (1, 2, 'baz', bar)"
         (select :foo (where (pk  [1 2 "baz" :bar])))))

  (is (= "SELECT * FROM foo WHERE keyalias = 'bar'"
         (select :foo (where (pk :keyalias "bar")))))

  (is (= "SELECT * FROM foo WHERE keyalias IN (1, 2, 'baz', bar)"
         (select :foo (where (pk :keyalias [1 2 "baz" :bar])))))

  (is (= "SELECT * FROM foo WHERE KEY > 1"
           (select :foo (where (pk {:gt 1})))))

  (is (= "SELECT * FROM foo WHERE KEY > 1 AND KEY <= 2"
         (select :foo (where (pk {:gt 1 :lte 2})))))


  (is (= "SELECT * FROM foo WHERE keyalias > 1"
           (select :foo (where (pk :keyalias {:gt 1})))))

  (is (= "SELECT * FROM foo WHERE keyalias > 1 AND keyalias <= 2"
         (select :foo (where (pk :keyalias {:gt 1 :lte 2}))))))

(deftest indexes-query
  (is (= "SELECT * FROM foo WHERE KEY = foo AND name>1 AND pwd='password' AND gender='male'"
         (select :foo (where
                       (pk :foo)
                       (columns [:gt "name" 1]
                                [:eq "pwd" "password"]
                                [:eq "gender" "male"]))))))

(deftest insert-query
  (is (="INSERT INTO foo (KEY, bar, alpha) VALUES (123, 'baz', 'beta') USING CONCISTENCY QUORUM AND TIMESTAMP 123123 AND TTL 123"
        (insert :foo
                (pk 123)
                (values {:bar "baz"
                         :alpha "beta"})
                (using  :concistency :QUORUM
                        :timestamp 123123
                        :TTL 123)))))

(deftest update-query
  (is (=  "UPDATE foo USING CONCISTENCY QUORUM AND TIMESTAMP 123123 AND TTL 123 SET col1 = 'value1', col2 = 'value2' WHERE pk-alias = 1"
          (update :foo
                  (where (pk :pk-alias 1))
                  (using  :concistency :QUORUM
                          :timestamp 123123
                          :TTL 123)
                  (values
                   {:col1 "value1"
                    :col2 "value2"})))))

(deftest delete-query
  (is (= "DELETE FROM foo USING CONCISTENCY QUORUM WHERE pk-alias = 1"
         (delete :foo
                 (columns [:a :b])
                 (where (pk :pk-alias 1))
                 (using  :concistency :QUORUM)))))


(deftest batch-query
  (is (= "BATCH BEGIN\n SELECT * FROM foo;INSERT INTO foo (KEY, bar, alpha) VALUES (123, 'baz', 'beta') USING CONCISTENCY QUORUM AND TIMESTAMP 123123 AND TTL 123;DELETE FROM foo USING CONCISTENCY QUORUM WHERE pk-alias = 1 \nAPPLY BATCH"
         (batch
          (select :foo)
          (insert :foo
                  (pk 123)
                  (values {:bar "baz"
                           :alpha "beta"})
                  (using  :concistency :QUORUM
                          :timestamp 123123
                          :TTL 123))
          (delete :foo
                  (columns [:a :b])
                  (where (pk :pk-alias 1))
                  (using  :concistency :QUORUM))))))