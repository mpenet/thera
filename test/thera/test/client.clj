(ns thera.test.client
  (:use [thera.client]
        [clojure.contrib.pprint :only [pprint]]))

(pprint (-> (make-datasource {})
             get-connection
             (prepare "SELECT * FROM user")
             execute-query
             resultset->clj))
