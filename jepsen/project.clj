(defproject phoenix-durable-channel-jepsen "0.1.0"
  :description "Jepsen test of phoenix-durable-channel's durable messages against the backend's session channel"
  :dependencies [[org.clojure/clojure "1.12.0"]
                 [jepsen "0.3.9"]
                 [cheshire "5.13.0"]]
  :jvm-opts ["-Xmx4g" "-Djava.awt.headless=true"]
  :main phoenix-durable-channel-jepsen.core)
