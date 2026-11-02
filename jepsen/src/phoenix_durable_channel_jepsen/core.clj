(ns phoenix-durable-channel-jepsen.core
  (:require [cheshire.core :as json]
            [clojure.java.shell :refer [sh]]
            [clojure.string :as str]
            [jepsen.checker :as checker]
            [jepsen.checker.timeline :as timeline]
            [jepsen.cli :as cli]
            [jepsen.client :as client]
            [jepsen.control :as c]
            [jepsen.control.core :as control]
            [jepsen.db :as db]
            [jepsen.generator :as gen]
            [jepsen.nemesis :as nemesis]
            [jepsen.nemesis.combined :as combined]
            [jepsen.net :as net]
            [jepsen.tests :as tests])
  (:import (java.net URI URLEncoder)
           (java.net.http HttpClient HttpRequest HttpRequest$BodyPublishers
                          HttpResponse$BodyHandlers)
           (java.time Duration)))

(def project "phoenix-durable-channel-jepsen")

(defn container-name
  [service]
  (str project "-" service "-1"))

(defn docker!
  [& arguments]
  (let [{:keys [exit out err]} (apply sh "docker" arguments)]
    (when-not (zero? exit)
      (throw (ex-info (str "docker " (str/join " " arguments) " failed: " err) {:exit exit})))
    out))

(defrecord DockerExec [container]
  control/Remote
  (connect [this conn-spec] (assoc this :container (container-name (:host conn-spec))))
  (disconnect! [this] this)
  (execute! [_ _context action]
    (apply sh "docker" "exec" "-i" container "bash" "-c" (:cmd action)
           (when-let [in (:in action)] [:in in])))
  (upload! [_ _context local-paths remote-path _options]
    (doseq [path (flatten [local-paths])]
      (docker! "cp" path (str container ":" remote-path))))
  (download! [_ _context remote-paths local-path _options]
    (doseq [path (flatten [remote-paths])]
      (docker! "cp" (str container ":" path) local-path))))

(def node-script
  (str (System/getenv "REPOSITORY") "/jepsen/docker/start-node.sh"))

(defn start-node!
  [test node]
  (let [peers (->> (:nodes test) (remove #{node}) (map #(str "jepsen@" %)) (str/join ","))]
    (c/exec* (str "pkill -CONT -x beam.smp || setsid nohup " node-script " 4000 "
                  (:durable-store test) " " peers " >>/tmp/node.log 2>&1 </dev/null &"))
    (c/exec* "timeout 120 bash -c 'until curl -s -o /dev/null localhost:4000/; do sleep 0.5; done'")))

(defn kill-node!
  [_test _node]
  (c/exec* "pkill -9 -x beam.smp || true")
  (c/exec* "while pgrep -x beam.smp >/dev/null; do sleep 0.1; done"))

(defn db
  []
  (reify
    db/DB
    (setup! [_ test node]
      (kill-node! test node)
      (start-node! test node))
    (teardown! [_ test node]
      (kill-node! test node))

    db/Kill
    (kill! [_ test node] (kill-node! test node))
    (start! [_ test node] (start-node! test node))

    db/Pause
    (pause! [_ _test _node] (c/exec* "pkill -STOP -x beam.smp || true"))
    (resume! [_ _test _node] (c/exec* "pkill -CONT -x beam.smp || true"))

    db/LogFiles
    (log-files [_ _test _node] {"/tmp/node.log" "node.log"})))

(def ^HttpClient http (HttpClient/newHttpClient))

(defn http!
  [^HttpRequest request]
  (let [response (.send http request (HttpResponse$BodyHandlers/ofString))]
    [(.statusCode response) (.body response)]))

(defn driver!
  [path body timeout-ms]
  (let [[status answer] (http! (-> (HttpRequest/newBuilder (URI. (str "http://127.0.0.1:4100" path)))
                                   (.timeout (Duration/ofMillis timeout-ms))
                                   (.header "content-type" "application/json")
                                   (.POST (HttpRequest$BodyPublishers/ofString (json/generate-string body)))
                                   (.build)))]
    (when-not (= 200 status)
      (throw (ex-info (str "driver " path ": " answer) {:status status})))
    (json/parse-string answer true)))

(defn topic [client] (str "jepsen:" client))

(defmulti read-stream
  (fn [test _client] (:durable-store test)))

(defmethod read-stream "mongo" [_test client]
  (->> (docker! "exec" (container-name "mongo") "mongosh" "--quiet" "jepsen" "--eval"
                (str "db.messages.find({topic: '" (topic client) "', event: 'added'}).sort({number: 1})"
                     ".forEach(m => print(m.payload.value + '|' + m.payload.position))"))
       str/split-lines
       (remove str/blank?)
       (mapv #(mapv parse-long (str/split % #"\|")))))

(defn read-token
  [stream]
  (let [encode #(.encodeToString (.withoutPadding (java.util.Base64/getUrlEncoder)) ^bytes %)
        part #(encode (.getBytes ^String (json/generate-string %) "UTF-8"))
        unsigned (str (part {:alg "HS256" :typ "JWT"}) "."
                      (part {:sub "phoenix-durable-channel-jepsen" :scope "read" :stream_id stream
                             :exp (+ 3600 (quot (System/currentTimeMillis) 1000))}))
        mac (doto (javax.crypto.Mac/getInstance "HmacSHA256")
              (.init (javax.crypto.spec.SecretKeySpec.
                      (.getBytes "phoenix-durable-channel-jepsen-signing-secret" "UTF-8") "HmacSHA256")))]
    (str unsigned "." (encode (.doFinal mac (.getBytes unsigned "UTF-8"))))))

(defmethod read-stream "durable-streams" [_test client]
  (loop [offset "-1" payloads []]
    (let [url (str "http://127.0.0.1:4252/v1/stream/phoenix-durable-channel-jepsen/"
                   (URLEncoder/encode ^String (topic client) "UTF-8") "?offset=" offset)
          response (.send http (-> (HttpRequest/newBuilder (URI. url))
                                   (.header "authorization" (str "Bearer " (read-token (topic client))))
                                   (.build))
                          (HttpResponse$BodyHandlers/ofString))
          header #(.orElse (.firstValue (.headers response) %) nil)]
      (when-not (= 200 (.statusCode response))
        (throw (ex-info (str "Durable Streams read " (.statusCode response) ": " (.body response)) {})))
      (let [payloads (into payloads (comp (filter #(= "added" (:event %)))
                                          (map #(let [payload (:payload %)] [(:value payload) (:position payload)])))
                           (json/parse-string (.body response) true))]
        (if (= "true" (header "stream-up-to-date"))
          payloads
          (recur (header "stream-next-offset") payloads))))))

(defmethod read-stream "s2" [_test client]
  (loop [from 0 payloads []]
    (let [url (str "http://127.0.0.1:4251/v1/streams/" (URLEncoder/encode ^String (topic client) "UTF-8")
                   "/records?seq_num=" from "&count=1000")
          [status body] (http! (-> (HttpRequest/newBuilder (URI. url))
                                   (.header "s2-basin" "phoenix-durable-channel-jepsen")
                                   (.header "s2-format" "raw")
                                   (.build)))]
      (case status
        416 payloads
        200 (let [records (:records (json/parse-string body true))
                  more (into [] (comp (map #(json/parse-string (:body %) true))
                                      (filter #(= "added" (:event %)))
                                      (map #(let [payload (:payload %)] [(:value payload) (:position payload)])))
                             records)]
              (if (empty? records)
                payloads
                (recur (inc (:seq_num (last records))) (into payloads more))))
        (throw (ex-info (str "S2 read " status ": " body) {:status status}))))))

(defn client-for
  [test op]
  (let [thread (mod (:process op) (:concurrency test))]
    (nth (:clients test) (mod thread (count (:clients test))))))

(defn wait-until!
  [timeout-ms done?]
  (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
    (loop []
      (when-not (or (done?) (> (System/currentTimeMillis) deadline))
        (Thread/sleep 500)
        (recur)))))

(defrecord Client []
  client/Client
  (open! [this _test _node] this)
  (setup! [_ test]
    (doseq [name (:clients test)]
      (driver! "/open" {:client name :heartbeatIntervalMs (:heartbeat-ms test)
                        :transport (:transport test)}
               10000)))
  (invoke! [_ test op]
    (let [name (client-for test op)]
      (case (:f op)
        :add
        (try
          (let [answer (driver! "/push" {:client name :value (:value op)} (:push-timeout-ms test))]
            (assoc op
                   :type (if (= "ok" (:status answer)) :ok :fail)
                   :client name
                   :position (:position answer)
                   :error (when-not (= "ok" (:status answer))
                            {:status (:status answer) :response (:response answer)})))
          (catch java.net.http.HttpTimeoutException _
            (assoc op :type :info :client name :error :timeout)))

        :read
        (do (wait-until! 120000 #(zero? (driver! "/unanswered" {} 10000)))
            (let [streams (into {} (map (juxt identity #(read-stream test %)) (:clients test)))]
              (wait-until! 60000 (fn []
                                   (let [delivered (driver! "/delivered" {} 10000)]
                                     (every? #(= (count (streams %)) (count (get delivered (keyword %))))
                                             (:clients test)))))
              (assoc op
                     :type :ok
                     :value {:streams streams
                             :delivered (update-keys (driver! "/delivered" {} 10000) clojure.core/name)}))))))
  (teardown! [_ _test])
  (close! [_ _test]))

(defn dedupe-adjacent
  [entries]
  (map first (partition-by identity entries)))

(defn client-problems
  [client stream delivered pushes]
  (let [handled (dedupe-adjacent stream)
        positions (map second handled)
        pushed (into {} (keep #(when (:position %) [(:position %) (:value %)]) pushes))
        acknowledged (keep #(when (= :ok (:type %)) (:position %)) pushes)
        problems (cond-> {}
                   (not= positions (range 1 (inc (count positions))))
                   (assoc :out-of-order positions)

                   (seq (remove #(<= % (count positions)) acknowledged))
                   (assoc :lost (sort (remove #(<= % (count positions)) acknowledged)))

                   (seq (remove (fn [[value position]] (= value (get pushed position value))) handled))
                   (assoc :unexpected (remove (fn [[value position]] (= value (get pushed position value))) handled))

                   (not= (map second stream) delivered)
                   (assoc :delivered-not-stream {:stream (map second stream) :delivered delivered}))]
    (when (seq problems) [client problems])))

(defn durable-checker
  []
  (reify checker/Checker
    (check [_ test history _opts]
      (let [completions (filter #(and (= :add (:f %)) (not= :invoke (:type %))) history)
            by-client (group-by :client completions)
            failed (filter #(= :fail (:type %)) completions)
            read (:value (first (filter #(and (= :read (:f %)) (= :ok (:type %))) history)))
            problems (into {} (keep #(client-problems % (get-in read [:streams %])
                                                      (get-in read [:delivered %])
                                                      (by-client %))
                                    (:clients test)))]
        {:valid? (and (some? read) (empty? problems) (empty? failed))
         :read? (some? read)
         :acknowledged-count (count (filter #(= :ok (:type %)) completions))
         :handled-count (reduce + (map #(count (dedupe-adjacent %)) (vals (:streams read))))
         :reruns (reduce + (map #(- (count %) (count (dedupe-adjacent %))) (vals (:streams read))))
         :delivered-count (reduce + (map count (vals (:delivered read))))
         :failed (map #(select-keys % [:value :client :position :error]) failed)
         :problems problems}))))

(defn gateway
  []
  (str/trim (c/exec* "ip route | awk '/default/ {print $3}'")))

(defn cut-off-clients-nemesis
  []
  (reify
    nemesis/Nemesis
    (setup! [this _test] this)
    (invoke! [_ test op]
      (case (:f op)
        :cut-off-clients
        (let [nodes (take (inc (rand-int 2)) (shuffle (:nodes test)))]
          (c/on-nodes test nodes
                      (fn [_ _]
                        (let [host (gateway)]
                          (c/exec* (str "iptables -w -A INPUT -s " host " -j DROP"))
                          (c/exec* (str "iptables -w -A OUTPUT -d " host " -j DROP")))))
          (assoc op :value nodes))
        :reconnect-clients
        (do (c/on-nodes test
                        (fn [_ _]
                          (let [host (gateway)]
                            (c/exec* (str "while iptables -w -D INPUT -s " host " -j DROP 2>/dev/null; do :; done"))
                            (c/exec* (str "while iptables -w -D OUTPUT -d " host " -j DROP 2>/dev/null; do :; done")))))
            (assoc op :value :all))))
    (teardown! [_ _test])
    nemesis/Reflection
    (fs [_] #{:cut-off-clients :reconnect-clients})))

(defn nemesis-package
  [opts]
  (let [faults (set (:faults opts))
        options {:db (:db opts)
                 :interval (:interval opts)
                 :faults faults
                 :partition {:targets [:one :majority :majorities-ring]}
                 :kill {:targets [:one :minority :all]}
                 :pause {:targets [:one :minority]}}
        cut-off? (faults :cut-off-clients)]
    (combined/compose-packages
     (cond-> [(combined/partition-package options) (combined/db-package options)]
       cut-off?
       (conj {:nemesis (cut-off-clients-nemesis)
              :generator (->> (cycle [{:type :info :f :cut-off-clients}
                                      {:type :info :f :reconnect-clients}])
                              (gen/stagger (:interval opts)))
              :final-generator [{:type :info :f :reconnect-clients}]
              :perf #{{:name "cut-off" :start #{:cut-off-clients} :stop #{:reconnect-clients}
                       :color "#E9A4A0"}}})))))

(defn durable-test
  [opts]
  (let [db (db)
        package (nemesis-package (assoc opts :db db))
        run (System/currentTimeMillis)]
    (merge tests/noop-test
           opts
           {:name (str "phoenix-durable-channel durable " (:durable-store opts) " " (:transport opts) " " (str/join "," (map name (:faults opts))))
            :remote (->DockerExec nil)
            :net net/iptables
            :db db
            :client (->Client)
            :nemesis (:nemesis package)
            :clients (mapv #(str run "-" %) (range (:client-count opts)))
            :checker (checker/compose {:durable (durable-checker)
                                       :perf (checker/perf {:nemeses (:perf package)})
                                       :stats (checker/stats)
                                       :exceptions (checker/unhandled-exceptions)
                                       :timeline (timeline/html)})
            :generator (gen/phases
                        (->> (map (fn [value] {:f :add :value value}) (range))
                             (gen/stagger (/ 1 (:rate opts)))
                             (gen/nemesis (gen/phases (gen/sleep 5) (:generator package)))
                             (gen/time-limit (:time-limit opts)))
                        (gen/nemesis (:final-generator package))
                        (gen/sleep 10)
                        (gen/clients (gen/once {:f :read})))})))

(def faults #{:partition :kill :pause :cut-off-clients})

(def cli-opts
  [[nil "--store STORE" "The server's ledger and stream: mongo (read through the channel), s2 or durable-streams."
    :id :durable-store
    :default "mongo"
    :validate [#{"mongo" "s2" "durable-streams"} "The store is mongo, s2 or durable-streams."]]
   [nil "--transport TRANSPORT" "The clients' transport: websocket or longpoll."
    :default "websocket"
    :validate [#{"websocket" "longpoll"} "The transport is websocket or longpoll."]]
   [nil "--faults FAULTS" "Comma-separated faults: partition, kill, pause, cut-off-clients."
    :default (vec faults)
    :parse-fn #(mapv keyword (str/split % #","))
    :validate [#(every? faults %) (str "Faults are " (str/join ", " (map name faults)))]]
   [nil "--interval SECONDS" "Seconds between faults." :default 10 :parse-fn parse-long]
   [nil "--rate HZ" "Pushes per second, across all clients." :default 20 :parse-fn parse-double]
   [nil "--client-count N" "Clients pushing." :default 5 :parse-fn parse-long]
   [nil "--heartbeat-ms MS" "The clients' heartbeat interval." :default 3000 :parse-fn parse-long]
   [nil "--push-timeout-ms MS" "How long a push's answer is waited for before it is :info."
    :default 30000 :parse-fn parse-long]])

(defn -main
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn durable-test :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))
