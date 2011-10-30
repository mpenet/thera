(ns thera.schema
  (:use [clojure.contrib.core :only [-?>]]))

(defprotocol PSchema
  (key-name-type [this])
  (key-value-type [this])
  (column-name-type [this])
  (exception [this name])
  (column-value-type [this] [this col-name]))

(defrecord Schema [name row-key columns]

  PSchema
  (key-name-type [this]
    (-> row-key :types first))

  (key-value-type [this]
    (-> row-key :types second))

  (column-name-type [this]
    (-> columns :types first))

  (column-value-type [this]
    (-> columns :types second))

  (exception [this name]
    (-?> columns :exceptions (get name)))

  (column-value-type [this name]
    (or (exception this name)
        (column-value-type this))))

(defn make-schema
  [name row-key columns]
  (Schema. name row-key columns))

(defmacro defschema
  [name & {:keys [row-key columns]}]
  `(make-schema ~(keyword name) ~row-key ~columns))