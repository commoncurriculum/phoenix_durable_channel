defmodule JepsenServer.Application do
  use Application

  def start(_type, _args) do
    children =
      Map.fetch!(
        %{
          PhoenixDurableChannel.Stream.Mongo => [
            {Mongo, name: JepsenServer.Mongo, url: "mongodb://127.0.0.1:27017/jepsen"},
            {Task, &PhoenixDurableChannel.Stream.Mongo.create_indexes/0}
          ],
          PhoenixDurableChannel.Stream.S2 => [
            {S2,
             name: JepsenServer.S2,
             basin: PhoenixDurableChannel.Stream.S2.config()[:basin],
             basin_endpoint: PhoenixDurableChannel.Stream.S2.config()[:basin_endpoint]}
          ],
          PhoenixDurableChannel.Stream.DurableStreams => [
            PhoenixDurableChannel.Stream.DurableStreams
          ]
        },
        Application.fetch_env!(:jepsen_server, :store)
      ) ++
        [
          {Phoenix.PubSub, name: JepsenServer.PubSub},
          {JepsenServer.Peers, Application.fetch_env!(:jepsen_server, :peers)},
          JepsenServer.Endpoint
        ]

    Supervisor.start_link(children, strategy: :one_for_one, name: JepsenServer.Supervisor)
  end
end

defmodule JepsenServer.Peers do
  use Task, restart: :permanent

  def start_link(peers), do: Task.start_link(__MODULE__, :connect, [peers])

  def connect(peers) do
    Enum.each(peers, &Node.connect/1)
    Process.sleep(1_000)
    connect(peers)
  end
end

defmodule JepsenServer.Endpoint do
  use Phoenix.Endpoint, otp_app: :jepsen_server

  @options [check_origin: false, serializer: [{PhoenixDurableChannel.Serializer, "~> 2.0.0"}]]

  socket("/socket", JepsenServer.UserSocket, websocket: @options, longpoll: @options)

  plug(:health)

  def health(connection, _options),
    do: connection |> Plug.Conn.send_resp(200, "ok") |> Plug.Conn.halt()
end

defmodule JepsenServer.UserSocket do
  use Phoenix.Socket

  channel("jepsen:*", JepsenServer.Channel)

  @impl true
  def connect(_params, socket, _connect_info), do: {:ok, socket}

  @impl true
  def id(_socket), do: nil
end

defmodule JepsenServer.Channel do
  use Phoenix.Channel

  use PhoenixDurableChannel.Channel,
    ledger: Application.fetch_env!(:jepsen_server, :store),
    stream: Application.fetch_env!(:jepsen_server, :store)

  @impl true
  def join(_topic, _params, socket), do: {:ok, socket}

  @impl true
  def handle_in("add", payload, socket) do
    push_durable(socket, "added", payload)
    {:noreply, socket}
  end
end
