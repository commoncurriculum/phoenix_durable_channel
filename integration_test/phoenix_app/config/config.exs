import Config

config :test_server, TestServer.Endpoint,
  adapter: Phoenix.Endpoint.Cowboy2Adapter,
  http: [ip: {127, 0, 0, 1}, port: String.to_integer(System.get_env("PORT", "4400"))],
  server: true,
  secret_key_base: String.duplicate("a", 64),
  pubsub_server: TestServer.PubSub

config :phoenix, :json_library, Jason
config :logger, level: :warning

config :phoenix_durable_channel, PhoenixDurableChannel.Stream.S2,
  name: TestServer.S2,
  basin: "phoenix-durable-channel",
  basin_endpoint: "http://127.0.0.1:4401",
  account_endpoint: nil,
  token: nil

config :phoenix_durable_channel, PhoenixDurableChannel.Stream.Mongo,
  name: TestServer.Mongo,
  messages: "messages",
  replies: "replies"

config :phoenix_durable_channel, PhoenixDurableChannel.Stream.DurableStreams,
  url: "http://127.0.0.1:#{System.get_env("PORT", "4400")}/durable-streams",
  reader_url: "http://127.0.0.1:4402",
  project: "phoenix-durable-channel",
  signing_secret: "phoenix-durable-channel-signing-secret"
