(ns thera.test.query
  (:refer-clojure :exclude [set])
  (:use [clojure.test]
        [thera.core]))

(deftest select-query
  (is (= ["SELECT * FROM foo" []]
         (as-cql (select :foo))))

  (is (= ["SELECT bar, ? FROM foo" ["baz"]]
         (as-cql (select :foo
                       (fields [:bar "baz"])))))

  (is (= ["SELECT REVERSED FIRST 100 FROM foo" []]
         (as-cql (select :foo
                       (fields :reversed true
                               :first 100)))))

  (is (= ["SELECT REVERSED FIRST 100 bar, ?, ? FROM foo" ["baz" "ab"]]
         (as-cql (select :foo
                       (fields [:bar "baz" (str "a" "b")]
                               :reversed true
                               :first 100)))))

  (is (= ["SELECT count() FROM foo" []]
         (as-cql (select :foo (fields [:count-fn])))))

  (is (= ["SELECT a...b FROM foo" []]
         (as-cql (select :foo (fields (as-range :a :b)))))))

(deftest using-query
  (is (= ["SELECT * FROM foo USING CONSISTENCY QUORUM and TIMESTAMP ? and TTL ?" [123123 123]]
         (as-cql (select :foo (using :consistency :QUORUM
                                   :timestamp 123123
                                   :TTL 123))))))

(deftest pks
  (is (= ["SELECT * FROM foo WHERE key = ?" [1]]
         (as-cql (select :foo (where (= key 1))))))

  (is (= ["SELECT * FROM foo WHERE key = ?" ["bar"]]
         (as-cql (select :foo (where (= key (str "ba" "r")))))))

  (is (= ["SELECT * FROM foo WHERE key in (?, ?, ?, bar)" [1 2 "baz"]]
         (as-cql (select :foo (where (in key [1 2 "baz" :bar]))))))

  (is (= ["SELECT * FROM foo WHERE keyalias = ?" ["bar"]]
         (as-cql (select :foo (where (= :keyalias "bar"))))))

  (is (= ["SELECT * FROM foo WHERE keyalias in (?, ?, ?, bar)" [1 2 "baz"]]
         (as-cql (select :foo (where
                             (in :keyalias [1 2 "baz" :bar]))))))

  (is (= ["SELECT * FROM foo WHERE key > ?" [1]]
         (as-cql (select :foo (where  (> key 1))))))

  (is (= ["SELECT * FROM foo WHERE key > ? and key <= ?" [1 2]]
         (as-cql (select :foo (where (and (> key 1)
                                        (<= key 2)))))))


  (is (= ["SELECT * FROM foo WHERE keyalias > ?" [1]]
         (as-cql (select :foo (where
                             (> :keyalias 1))))))

  (is (= ["SELECT * FROM foo WHERE keyalias > ? and keyalias <= ?" [1 2]]
         (as-cql (select :foo (where
                             (and (> :keyalias 1)
                                  (<= :keyalias 2))))))))

(deftest indexes-query
  (is (= ["SELECT * FROM foo WHERE key = foo and name > ? and pwd = ? and gender = ?" [1 "password" "male"]]
         (as-cql (select :foo
                       (where
                        (and
                         (= key :foo)
                         (> :name 1)
                         (= :pwd "password")
                         (= :gender "male"))))))))

(deftest insert-query
  (is (= ["INSERT INTO foo (key, bar, alpha) VALUES (?, ?, ?) USING CONSISTENCY QUORUM and TIMESTAMP ? and TTL ?" [123 "baz" "beta" 123123 123]]
         (as-cql (insert :foo
                       (values {:key 123
                                :bar "baz"
                                :alpha "beta"})
                       (using  :consistency :QUORUM
                               :timestamp 123123
                               :TTL 123))))))

(deftest update-query
  (is (=  ["UPDATE foo USING CONSISTENCY QUORUM and TIMESTAMP ? and TTL ? SET col1 = ?, col2 = ? WHERE key-alias = ?" [123123 123 "value1" "value2" 1]]
          (as-cql (update :foo
                        (where (= :key-alias 1))
                        (using  :consistency :QUORUM
                                :timestamp 123123
                                :TTL 123)
                        (set
                         {:col1 "value1"
                          :col2 "value2"})))))

  (is (= ["UPDATE foo USING CONSISTENCY QUORUM and TIMESTAMP ? and TTL ? SET col1 = ?, col2 = ?, col3 = col3 + ? WHERE pk-alias = ?" [123123 123 "value1" "value2" 100 1]]
         (as-cql (update :foo
                       (where (= :pk-alias 1))
                       (using  :consistency :QUORUM
                               :timestamp 123123
                               :TTL 123)
                       (set
                        {:col1 "value1"
                         :col2 "value2"
                         :col3 (+= 100)}))))))

(deftest delete-query
  (is (= ["DELETE a, b FROM foo USING CONSISTENCY QUORUM WHERE pk-alias = ?" [1]]
         (as-cql (delete :foo
                       (fields [:a :b])
                       (where (= :pk-alias 1))
                       (using  :consistency :QUORUM))))))


;; (deftest batch-query
;;   (is (= "BATCH BEGIN\n SELECT * FROM foo;INSERT INTO foo (key, bar, alpha) VALUES (123, 'baz', 'beta') USING CONSISTENCY QUORUM and TIMESTAMP 123123 and TTL 123;DELETE a, b FROM foo USING CONSISTENCY QUORUM WHERE pk-alias = 1 \nAPPLY BATCH"
;;          (batch
;;           (select :foo)
;;           (insert :foo
;;                   (= key 123)
;;                   (values {:bar "baz"
;;                            :alpha "beta"})
;;                   (using  :consistency :QUORUM
;;                           :timestamp 123123
;;                           :TTL 123))
;;           (delete :foo
;;                   (fields [:a :b])
;;                   (where (= pk-alias 1))
;;                   (using  :consistency :QUORUM))))))