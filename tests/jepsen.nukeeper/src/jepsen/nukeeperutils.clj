(ns jepsen.nukeeperutils
  (:require [clojure.string :as str]
            [zookeeper.data :as data]
            [zookeeper :as zk]))

(defn client-url
  [node]
  (str node ":9181"))

(defn parse-long
  "Parses a string to a Long. Passes through `nil` and empty strings."
  [s]
  (if (and s (> (count s) 0))
    (Long/parseLong s)))

(defn parse-zk-long
  [val]
  (parse-long (data/to-string val)))

(defn zk-range
  []
  (map (fn [v] (str "/" v)) (range)))

(defn zk-path
  [n]
  (str "/" n))

(defn zk-cas
  [zk path old-value new-value]
  (let [current-value (zk/data zk path)]
    (if (= (parse-zk-long (:data current-value)) old-value)
      (do (zk/set-data zk path (data/to-bytes (str new-value)) (:version (:stat current-value)))
          true))))
