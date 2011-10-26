(ns thera.test.schema

  (:use [thera.schema]
        [thera.client]))

(def test-schema (defschema user  :row-key {:types [:string :string]}
                   :columns {:type :bytes
                             :exceptions {:name :string
                                          :date :integer}}

                   :consistency {:default :ONE
                                 :read :ANY
                                 :create :QUORUM
                                 :update :ALL
                                 :delete :ANY}
                   :pre identity
                   :post identity))


(print (def rs1 (-> (make-datasource {})
                    get-connection
                    (prepare "SELECT * FROM user WHERE KEY")
                    execute-query )))

(thera.client/resultset->clj thera.test.schema/rs1 test-schema)