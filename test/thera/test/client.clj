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


;; (def s
;;   (select :user (pk 1)))



;; (def s (-> (make-datasource {})
;;              get-connection
;;              (prepare-statement "SELECT  * FROM user")
;;              execute
;;              ;; (resultset->result :decoder :guess)
;;              (resultset->result :decoder :bytes)

;;              ))

;; (println s)

;; (def s (-> (make-datasource {})
;;              get-connection
;;              (prepare-statement "SELECT  * FROM user")
;;              execute
;;              ;; (resultset->result :decoder :guess)
;;              (resultset->result :decoder :hex)

;;              ))

;; (println s)


;; (def s (-> (make-datasource {})
;;              get-connection
;;              (prepare-statement "SELECT  * FROM sample1")
;;              execute
;;              ;; (resultset->result :decoder :guess)
;;              (resultset->result :decoder :bytes)

;;              ))




(def s (-> (make-datasource {})
             get-connection
             (prepare-statement "SELECT * FROM user WHERE KEY=8")
             execute
             ;; (resultset->result :decoder :guess)
             (decode-result :schema user-schema)))

;; (println (-> s :rows first :cols first :value type))

(def s (-> (make-datasource {})
             get-connection
             (prepare-statement "SELECT * FROM sample1")
             execute
             ;; (resultset->result :decoder :guess)
             (decode-result :schema sample-schema)

             ))

;; (def s (-> (make-datasource {})
;;              get-connection
;;              (prepare-statement "SELECT * FROM sample1 LIMIT 1")
;;              execute
;;              ;; (resultset->result :decoder :guess)
;;              (decode-result :bytes)
;;              ))

(pprint (-> s :rows))


;; (println (insert :user
;;                  (pk :id  "1438fc5c-4ff6-11e0-b97f-0026c650d726")
;;                  (values {:username "mememe"})))


;; (println (-> (make-datasource {})
;;      get-connection
;;      (prepare-statement (insert :sample1
;;                                 (pk "1438fc5c-4ff6-11e0-b97f-0026c650d726")
;;                                 (values {:username "mememe"
;;                                          :age 12 :birthdate 44})))
;;      execute
;;      ;; (resultset->result :decoder :guess)
;;      ;; (decode-result :schema sample-schema)
;;      ))

;; (println (-> (make-datasource {})
;;      get-connection
;;      (prepare-statement (insert :user
;;                                 (pk 8)
;;                                 (values {:dwa (encode :utf-8 "gni")
;;                                          :pwd (long 123456789010)})))
;;      execute
;;      ;; (resultset->result :decoder :guess)
;;      ;; (decode-result :schema sample-schema)
;;      ))



;; (println (encode :utf-8  "12345678"))
;; (println (encode :long  12345678))


;; (println "ASD"(decode :long (.getBytes (encode :utf-8  "12345678"))))