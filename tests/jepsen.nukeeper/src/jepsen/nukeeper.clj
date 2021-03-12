(ns jepsen.nukeeper
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen
             [checker :as checker]
             [cli :as cli]
             [client :as client]
             [control :as c]
             [db :as db]
             [nemesis :as nemesis]
             [generator :as gen]
             [independent :as independent]
             [tests :as tests]]
            [jepsen.control.util :as cu]
            [jepsen.os.ubuntu :as ubuntu]
            [jepsen.checker.timeline :as timeline]
            [clojure.java.io :as io]
            [knossos.model :as model]
            [zookeeper.data :as data]
            [slingshot.slingshot :refer [try+]]
            [zookeeper :as zk])
  (:import (org.apache.zookeeper ZooKeeper KeeperException KeeperException$BadVersionException)))

(def dir "/var/lib/clickhouse")
(def binary "clickhouse")
(def logdir "/var/log/clickhouse-server")
(def logfile "/var/log/clickhouse-server/stderr.log")
(def serverlog "/var/log/clickhouse-server/clickhouse-server.log")
(def pidfile (str dir "/clickhouse.pid"))
(def binary-path "/tmp")

(defn cluster-config
  [test node config-template]
  (let [nodes (:nodes test)]
    (clojure.string/replace
     (clojure.string/replace
      (clojure.string/replace
       (clojure.string/replace
        (clojure.string/replace config-template #"\{quorum_reads\}" (str (boolean (:quorum test))))
        #"\{srv1\}" (get nodes 0))
       #"\{srv2\}" (get nodes 1))
      #"\{srv3\}" (get nodes 2))
     #"\{id\}" (str (inc (.indexOf nodes node))))))

(defn db
  [version]
  (reify db/DB
    (setup! [_ test node]
      (info node "installing clickhouse" version)
      (c/su
       (if-not (cu/exists? (str binary-path "/clickhouse"))
         (c/exec :sky :get :-d binary-path :-N :Backbone version))
       (c/exec :mkdir :-p logdir)
       (c/exec :touch logfile)
       (c/exec (str binary-path "/clickhouse") :install)
       (c/exec :chown :-R :root dir)
       (c/exec :chown :-R :root logdir)
       (c/exec :echo (slurp (io/resource "listen.xml")) :> "/etc/clickhouse-server/config.d/listen.xml")
       (c/exec :echo (cluster-config test node (slurp (io/resource "test_keeper_config.xml"))) :> "/etc/clickhouse-server/config.d/test_keeper_config.xml")
       (cu/start-daemon!
        {:pidfile pidfile
         :logfile logfile
         :chdir dir}
        (str binary-path "/clickhouse")
        :server
        :--config "/etc/clickhouse-server/config.xml")
       (Thread/sleep 10000)))

    (teardown! [_ test node]
      (info node "tearing down clickhouse")
      (cu/stop-daemon! (str binary-path "/clickhouse") pidfile)
      (c/su
       (c/exec :rm :-f (str binary-path "/clickhouse"))
       (c/exec :rm :-rf dir)
       (c/exec :rm :-rf logdir)
       (c/exec :rm :-rf "/etc/clickhouse-server")))

    db/LogFiles
    (log-files [_ test node]
      [logfile serverlog])))

(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

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

(defn zk-cas
  [zk path old-value new-value]
  (let [current-value (zk/data zk path)]
    (if (= (parse-zk-long (:data current-value)) old-value)
      (do (zk/set-data zk path (data/to-bytes (str new-value)) (:version (:stat current-value)))
          true))))

(defn zk-range
  []
  (map (fn [v] (str "/" v)) (range)))

(defn zk-path
  [n]
  (str "/" n))

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (zk/connect (client-url node) :timeout-msec 30000)))

  (setup! [this test]
    (dorun (map (fn [v] (zk/create-all conn v)) (take 300 (zk-range)))))

  (invoke! [_ test op]
    (let [[k v] (:value op)
          zk-k (zk-path k)]
      (case (:f op)
        :read (try
                (assoc op :type :ok, :value (independent/tuple k (parse-zk-long (:data (zk/data conn zk-k)))))
                (catch Exception _ (assoc op :type :fail, :error :connect-error)))
        :write (try
                 (do (zk/set-data conn zk-k (data/to-bytes (str v)) -1)
                     (assoc op :type :ok))
                 (catch Exception _ (assoc op :type :info, :error :connect-error)))
        :cas (try
               (let [[old new] v]
                 (assoc op :type (if (zk-cas conn zk-k old new)
                                   :ok
                                   :fail)))
               (catch KeeperException$BadVersionException _ (assoc op :type :fail, :error :bad-version))
               (catch Exception _ (assoc op :type :info, :error :connect-error))))))

  (teardown! [this test])

  (close! [_ test]
    (zk/close conn)))

(def cli-opts
  "Additional command line options."
  [["-q" "--quorum" "Use quorum reads, instead of reading from any primary."]
   ["-r" "--rate HZ" "Approximate number of requests per second, per thread."
    :default  10
    :parse-fn read-string
    :validate [#(and (number? %) (pos? %)) "Must be a positive number"]]
   [nil "--ops-per-key NUM" "Maximum number of operations on any given key."
    :default  100
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer."]]])

(defn nukeeper-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (merge tests/noop-test
         opts
         {:name "clickhouse"
          :os ubuntu/os
          :db (db "rbtorrent:8831b5baa571abc28340cf66a9279a4ce45fac64")
          :pure-generators true
          :client (Client. nil)
          :nemesis (nemesis/partition-random-halves)
          :checker (checker/compose
                    {:perf   (checker/perf)
                     :indep (independent/checker
                             (checker/compose
                              {:linear (checker/linearizable {:model     (model/cas-register)
                                                              :algorithm :linear})
                               :timeline (timeline/html)}))})
          :generator (->> (independent/concurrent-generator
                           10
                           (range)
                           (fn [k]
                             (->> (gen/mix [r w cas])
                                  (gen/stagger (/ (:rate opts)))
                                  (gen/limit (:ops-per-key opts))))) ;otherwise our history will be very long
                          (gen/nemesis
                           (cycle [(gen/sleep 5)
                                   {:type :info, :f :start}
                                   (gen/sleep 5)
                                   {:type :info, :f :stop}]))
                          (gen/time-limit (:time-limit opts)))}))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn nukeeper-test
                                         :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))
