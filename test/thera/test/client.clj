(ns thera.test.client
  (:use [thera client schema query codec]
        [clojure.contrib.pprint :only [pprint]]))


(def user-schema
  (defschema user
    :row-key {:types [:utf-8 :integer]
              :alias :foo}

    :columns {:types [:utf-8 :integer]
              :exceptions {"pwd" :utf-8 "dwa" :utf-8}}))

(def sample-schema
  (defschema sample
    :row-key {:types [:utf-8 :uuid]
              :alias :foo}

    :columns {:types [:utf-8 :utf-8]
              :exceptions {"age" :integer
                           "birthdate" :long
                           "id" :uuid
                           "KEY" :uuid
                           "username" :utf-8}}))

;; (def q (select :user (where (= key (.toByteArray (BigInteger/valueOf 8))))))


;; (def s (-> (make-datasource {})
;;              get-connection
;;              (prepare-statement (q 0))
;;              (bind-parameters (q 1))
;;              execute
;;              (decode-result :client-schema user-schema)))

;; (println (-> s :rows first :cols first :value type))

(def q (select :sample1 (where (= :key (java.util.UUID/fromString "1438fc5c-4ff6-11e0-b97f-0026c650d723")))))

(def s (-> (make-datasource {})
             get-connectiony
             (prepare-statement (q 0))
             (bind-parameters (q 1))
             execute
             (decode-result :server-schema sample-schema)))

(println (-> s :rows first))


;; (println (select :foo (where (and (> :key (str "A" "A")) (= :keyalias (str "dwa" 1))))))


;; (println  (select :foo (where (in key [1 2 "baz" :bar]))))


;; (println (select :foo (where (= key (java.util.UUID/fromString "1438fc5c-4ff6-11e0-b97f-0026c650d722")))))


(-> (select* :foo)
    (where (= key 1))
    (using :)
    )