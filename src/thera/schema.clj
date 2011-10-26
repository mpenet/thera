(ns thera.schema)

(comment
  (defschema User

    :row-key {:types [:string :string]
              :alias :foo}

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

(defprotocol PSchema
  (row-name-type [this])
  (row-value-type [this])
  (column-type [this] [this col-name])
  (consistency [this] [this query-type]))

(defrecord Schema [name row-key columns consistency pre post]

  PSchema
  (row-name-type [this]
    (-> row-key :types first))

  (row-value-type [this]
    (-> row-key :types second))

  (column-type [this]
    (:type columns))

  (column-type [this name]
    (get-in columns [:exceptions name]
            (column-type this)))

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
