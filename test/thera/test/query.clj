(ns thera.test.query
  (:use [clojure.test]
        [thera.query]))

(deftest select-query
  (is (= "SELECT * FROM foo"
         (select :foo)))

  (is (= "SELECT bar, 'baz' FROM foo"
         (select :foo
              (fields [:bar "baz"]))))

  (is (= "SELECT REVERSED FIRST 100 FROM foo"
         (select :foo
                 (fields :reversed true
                         :first 100))))

  (is (= "SELECT REVERSED FIRST 100 bar, 'baz' FROM foo"
         (select :foo
                 (fields [:bar "baz"]
                         :reversed true
                         :first 100))))

  (is (= "SELECT count() FROM foo"
         (select :foo (fields [:count-fn]))))

  (is (= "SELECT a...b FROM foo"
         (select :foo (fields (as-range :a :b))))))

(deftest using-query
  (is (= "SELECT * FROM foo USING CONSISTENCY QUORUM and TIMESTAMP 123123 and TTL 123"
         (select :foo (using :consistency :QUORUM
                             :timestamp 123123
                             :TTL 123)))))

(deftest pks
  (is (= "SELECT * FROM foo WHERE key = 1"
         (select :foo (where (= key 1)))))

  (is (= "SELECT * FROM foo WHERE key = 'bar'"
         (select :foo (where (= key "bar")))))

  (is (= "SELECT * FROM foo WHERE key in (1, 2, 'baz', bar)"
         (select :foo (where (in key [1 2 "baz" :bar])))))

  (is (= "SELECT * FROM foo WHERE keyalias = 'bar'"
         (select :foo (where (= keyalias "bar")))))

  (is (= "SELECT * FROM foo WHERE keyalias in (1, 2, 'baz', bar)"
         (select :foo (where
                       (in keyalias [1 2 "baz" :bar])))))

  (is (= "SELECT * FROM foo WHERE key > 1"
           (select :foo (where (> key 1)))))

  (is (= "SELECT * FROM foo WHERE key > 1 and key <= 2"
         (select :foo (where (and (> key 1)
                                  (<= key 2))))))


  (is (= "SELECT * FROM foo WHERE keyalias > 1"
         (select :foo (where
                       (> keyalias 1)))))

  (is (= "SELECT * FROM foo WHERE keyalias > 1 and keyalias <= 2"
         (select :foo (where
                       (and (keyalias > 1)
                            (<= keyalias 2)))))))

(deftest indexes-query
  (is (= "SELECT * FROM foo WHERE key = foo and name > 1 and pwd = 'password' and gender = 'male'"
         (select :foo
                 (where
                  (and
                   (= key :foo)
                   (> :name 1)
                   (= :pwd "password")
                   (= :gender "male")))))))

(deftest insert-query
  (is (="INSERT INTO foo (key, bar, alpha) VALUES (123, 'baz', 'beta') USING CONSISTENCY QUORUM and TIMESTAMP 123123 and TTL 123"
        (insert :foo
                (= key 123)
                (values {:bar "baz"
                         :alpha "beta"})
                (using  :consistency :QUORUM
                        :timestamp 123123
                        :TTL 123)))))

(deftest update-query
  (is (=  "UPDATE foo USING CONSISTENCY QUORUM and TIMESTAMP 123123 and TTL 123 SET col1 = 'value1', col2 = 'value2' WHERE key-alias = 1"
          (update :foo
                  (where (= key-alias 1))
                  (using  :consistency :QUORUM
                          :timestamp 123123
                          :TTL 123)
                  (values
                   {:col1 "value1"
                    :col2 "value2"}))))

  (is (= "UPDATE foo USING CONSISTENCY QUORUM and TIMESTAMP 123123 and TTL 123 SET col1 = 'value1', col2 = 'value2', col3 = col3 + 100 WHERE pk-alias = 1"
         (update :foo
                 (where (= pk-alias 1))
                 (using  :consistency :QUORUM
                         :timestamp 123123
                         :TTL 123)
                 (values
                  {:col1 "value1"
                   :col2 "value2"
                   :col3 (+ 100)})))))

(deftest delete-query
  (is (= "DELETE a, b FROM foo USING CONSISTENCY QUORUM WHERE pk-alias = 1"
         (delete :foo
                 (fields [:a :b])
                 (where (= pk-alias 1))
                 (using  :consistency :QUORUM)))))


(deftest batch-query
  (is (= "BATCH BEGIN\n SELECT * FROM foo;INSERT INTO foo (key, bar, alpha) VALUES (123, 'baz', 'beta') USING CONSISTENCY QUORUM and TIMESTAMP 123123 and TTL 123;DELETE a, b FROM foo USING CONSISTENCY QUORUM WHERE pk-alias = 1 \nAPPLY BATCH"
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
                  (using  :consistency :QUORUM))))))