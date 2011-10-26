(ns thera.test.schema
  (:use [thera.schema]
        [thera.client]))

(def test-schema (defschema user  :row-key {:types [:utf8 :integer]}
                   :columns {:types [:utf8 :utf8]
                             :exceptions {"pwd" :integer
                                          "name" :integer}}

                   :consistency {:default :ONE
                                 :read :ANY
                                 :create :QUORUM
                                 :update :ALL
                                 :delete :ANY}
                   :pre identity
                   :post identity))



(def rs1 (-> (make-datasource {})
                   get-connection
                   (prepare "SELECT * FROM user")
                   execute-query ))

(def t3 (thera.client/resultset->clj thera.test.schema/rs1 test-schema))