import Config

# docker/start-node.sh sets these on each node.
config :jepsen_server, JepsenServer.Endpoint,
  http: [ip: {0, 0, 0, 0}, port: String.to_integer(System.fetch_env!("PORT"))]

config :jepsen_server,
  store:
    Map.fetch!(
      %{
        "mongo" => PhoenixDurableChannel.Stream.Mongo,
        "s2" => PhoenixDurableChannel.Stream.S2,
        "durable-streams" => PhoenixDurableChannel.Stream.DurableStreams
      },
      System.fetch_env!("STORE")
    ),
  peers:
    "PEERS" |> System.fetch_env!() |> String.split(",", trim: true) |> Enum.map(&String.to_atom/1)
