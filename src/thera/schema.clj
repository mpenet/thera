(ns thera.schema
  (:use [clojure.contrib.core :only [-?>]])
  )

(comment
  (defschema User

    :row-key {:types [:string :string]
              :alias :foo}

    :columns {:types [:string :string]
              :exceptions {"date" :integer}}

    ;; :columns {:types [:keyword :string]
    ;;           :exceptions {:date :integer}}

    :consistency {:default :ONE
                  :read :ANY
                  :create :QUORUM
                  :update :ALL
                  :delete :ANY}
    :pre identity
    :post identity))

(defprotocol PSchema
  (row-name-type [this])
  (row-value-type [this])
  (column-name-type [this])
  (column-value-type [this] [this col-name])
  (consistency [this] [this query-type]))

(defrecord Schema [name row-key columns consistency pre post]

  PSchema
  (row-name-type [this]
    (-> row-key :types first))

  (row-value-type [this]
    (-> row-key :types second))

  (column-name-type [this]
    (-> columns :types first))

  (column-value-type [this]
    (-> columns :types second))

  (column-value-type [this name]
     (or (-?> columns :exceptions (get name))
        (column-value-type this)))

  (consistency [this]
    (:default consistency))

  (consistency [this query-type]
    (get consistency query-type (consistency this))))

(defn make-schema
  [name row-key column-type consistency pre post]
  (Schema. name row-key column-type consistency pre post))

(defmacro defschema
  [name & {:keys [row-key columns consistency pre post]}]
  `(make-schema ~(keyword name) ~row-key ~columns ~consistency ~pre ~post))