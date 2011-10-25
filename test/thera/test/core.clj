(ns thera.test.core
  (:use [thera.core])
  (:use [clojure.test]))

(prn (select :user
         (limit 100)
         (using  :concistency :QUORUM
                 :timestamp 123123
                 :TTL 123)
         (fields (as-range :a :b)
                  :reversed true
                  :first 100)

         ;; (fields [:a :b]
         ;;          :reversed true
         ;;          :first 100)
         (where
          (pk 1)

          (columns [:gt "name" 2]
                   [:eq :a "b"]
                   [:lt :a "b"])
          ;; (pk "a")

          ;; (pk [1 2 "a" 4])
          ;; (pk "1" [1 2 "a" 4])

          ;; (pk :kalias1 1)
          ;; (pk :kalias1 "a")

          ;; (pk :kalias1 [1 2 3 4])
          ;; (pk :kalias1 [1 2 "a" 4])

          ;; (pk {:gt 1 :lte 2})

          ;; (pk {:gt 1 :lte 2})
          ;; (pk {:gt 1})

          ;; (pk :keyalias1
          ;;     {:gt 1})

          ;; (pk :alias {:gt 1 :lt 2})

          ;; (cols {:name "value"})
          ;; (cols {:name {:gt 1}})
          )))

(prn (insert :users
             (pk :pk-alias 1)
             (using  :concistency :QUORUM
                     :timestamp 123123
                     :TTL 123)
             (values
              {:col1 "value1"
               :col2 "value2"})))


(prn (update :users
             (where (pk :pk-alias 1))
             (using  :concistency :QUORUM
                     :timestamp 123123
                     :TTL 123)
             (values
              {:col1 :test
               :col2 "value2"})))


(prn (delete :users
             (columns [:a :b])
             (where (pk :pk-alias 1))
             (using  :concistency :QUORUM)))

(prn

 (batch
  (using  :concistency :QUORUM
          :timestamp 123123
          :TTL 123)
  (delete :users
          (columns [:a :b])
          (where (pk :pk-alias 1))
          (using  :concistency :QUORUM))
  (delete :users
          (columns [:a :b])
          (where (pk :pk-alias 2))
          (using  :concistency :QUORUM))
  (delete :users
          (columns [:a :b])
          (where (pk :pk-alias 3))
          (using  :concistency :QUORUM))))



;; (prn (insert :users
;;              1
;;              (values
;;               {:col1 "value"
;;                :col2 "value2"})))


;; (prn (select :user
;;          (limit 100)
;;          ;; (using :concistency :QUORUM
;;          ;;        :timestamp 123123
;;          ;;        :TTL 123)
;;          (columns [:a :b]
;;                   :first 100
;;                   :reversed true)
;;          (where (= :key "name")
;;                 (AND
;;                  (= :name1 1)
;;                  (= :name2 "value2")))

;;          ))



;; (prn (select :user
;;          (limit 100)

;;          (columns [:a :b]
;;                   :reversed true
;;                   :first 100)
;;          (where (in :key [:a :b :c]))))


;; (prn (insert :user
;;              ({:akey {:a 1 :b 2}})
;;              (where (in :key [:a :b :c]))))