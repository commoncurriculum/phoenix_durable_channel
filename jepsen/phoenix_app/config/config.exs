import Config

config :jepsen_server, JepsenServer.Endpoint,
  adapter: Phoenix.Endpoint.Cowboy2Adapter,
  server: true,
  secret_key_base: String.duplicate("a", 64),
  pubsub_server: JepsenServer.PubSub

config :phoenix, :json_library, Jason
config :logger, level: :info

config :phoenix_durable_channel, PhoenixDurableChannel.Stream.Mongo,
  name: JepsenServer.Mongo,
  messages: "messages",
  replies: "replies"

config :phoenix_durable_channel, PhoenixDurableChannel.Stream.S2,
  name: JepsenServer.S2,
  basin: "phoenix-durable-channel-jepsen",
  basin_endpoint: "http://127.0.0.1:4251",
  account_endpoint: nil,
  token: nil

config :phoenix_durable_channel, PhoenixDurableChannel.Stream.DurableStreams,
  url: "http://127.0.0.1:4252",
  project: "phoenix-durable-channel-jepsen",
  signing_secret: "phoenix-durable-channel-jepsen-signing-secret"
