(ns thera.test.client
  (:use [thera client schema]
        [clojure.contrib.pprint :only [pprint]]))


(def user-schema
  (defschema user
    :row-key {:types [:string :string]
              :alias :foo}

    :columns {:types [:string :integer]
              :exceptions {"pwd" :long}}))

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
             (prepare-statement "SELECT * FROM user WHERE KEY=3")
             execute
             ;; (resultset->result :decoder :guess)
             (resultset->result :decoder :schema :as user-schema)

             ))

(def s (-> (make-datasource {})
             get-connection
             (prepare-statement "SELECT * FROM sample1 LIMIT 1")
             execute
             ;; (resultset->result :decoder :guess)
             (resultset->result :decoder :bytes)

             ))

(println  s)





(pprint (-> (make-datasource {})
             get-connection
             (prepare-statement "INSERT INTO user (KEY, 647761, pwd) VALUES (3, 2, 3)")
             execute))
