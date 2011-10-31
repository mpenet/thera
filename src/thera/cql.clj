(ns thera.cql
  ^{:doc "Query maps to CQL transformations"}
  (:use [clojure.string :only [upper-case join]]))

(def default-key-name "KEY")
(def join-and (partial join " AND "))
(def join-spaced (partial join " "))
(def join-coma (partial join ", "))

(def operators
  {:gt ">"
   :lt "<"
   :lte "<="
   :gte ">="
   :eq "="})

(defprotocol PEncoder
  (encode-value [value])
  (encode-name [value]))

(extend-protocol PEncoder

  java.lang.String
  (encode-value [value]
    (format "'%s'" value))
  (encode-name [value] value)

  clojure.lang.Keyword
  (encode-value [value] (name value))
  (encode-name [value] (name value))

  clojure.lang.Sequential
  (encode-value [value]
    (format "(%s)" (join-coma (map encode-value value))))
  (encode-name [value]
    (format "(%s)" (join-coma (map encode-name value))))

  clojure.lang.IPersistentMap
  (encode-value [value]
    (->> (map (fn [[k v]] (format "%s = %s" (encode-name k) (encode-value v))) value)
         (join-coma)))

  java.lang.Object
  (encode-value [value] value)
  (encode-name [value] value))

(defmulti translate (fn [token value] token))

(defmethod translate :column-family
  [_ column-family]
  (encode-name column-family))

(defmethod translate :fields
  [_ args]
  (let [[columns opts] (if (or (vector? (first args))
                               (map? (first args)))
                         [(first args) (rest args)]
                         [nil args])]
    (->> [(when (seq opts) (translate :fields-options (apply array-map opts)))
         (when (seq columns) (translate :fields-value columns))]
         (filter identity)
         join-spaced)))

(defmethod translate :fields-value
  [_ value]
  (if (map? value)
    (let [range (:range value)]
      (format "%s...%s"
              (-> range first encode-name)
              (-> range second encode-name)))
    (join-coma (map encode-name value))))

(defmethod translate :fields-options
  [_ opts]
  (join-spaced (filter identity (map #(apply translate %) opts))))

(defmethod translate :first [_ first]
  (when first (str "FIRST " first)))

(defmethod translate :reversed
  [_ reversed]
  (when reversed "REVERSED"))

(defmethod translate :using
  [_ args]
  (str "USING "
       (join-and
        (for [[n value] (partition 2 args)]
          (str (-> n name upper-case)
               " " (encode-value value))))))

(defmethod translate :pk
  [_ value]
  (let [pk-name (if (and (= 2 (count value))
                         (not (map? (first value))))
                  (-> value first encode-name)
                  default-key-name)
        pk-value (last value)]
    (cond
     (vector? pk-value)
     (format "%s IN %s" pk-name (encode-value pk-value))

     (map? pk-value)
     (join-and
      (map (fn [[op v]]
             (join-spaced [pk-name (op operators) v]))
           pk-value))
     :else
     (str pk-name " = " (encode-value pk-value)))))

(defmethod translate :where
  [_ where]
  (str "WHERE "
       (join-and
        (map (fn [[k v]]
               (translate k v))
             ;; ensure pk is first
             (reverse (sort-by key where))))))

(defmethod translate :insert-values [_ {:keys [row values]}]
  (let [[key-name key-value] [(if (= 2 (count row))
                                (first row)
                                default-key-name)
                              (last row)]]
    (format "%s VALUES %s"
            (->> values keys (cons key-name) encode-name)
            (->> values vals (cons key-value) encode-value))))

(defmethod translate :columns [_ columns]
  (join-and (map (fn [[op k v]]
                   (str (encode-name k) (op operators) (encode-value v)))
                 columns)))

(defmethod translate :set [_ values]
  (str "SET " (encode-value values)))

(defmethod translate  :limit
  [_ limit]
  (str "LIMIT " limit))

(defmethod translate :queries [_ queries]
  (join ";" queries))

(defn make-query
  [template query]
  (println "Q:" query)
  (->> (map (fn [token]
              (if (string? token)
                token
                (when-let [value (token query)]
                  (translate token value))))
            template)
       (filter identity)
       join-spaced))
