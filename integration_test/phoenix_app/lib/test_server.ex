defmodule TestServer.Application do
  use Application

  def start(_type, _args) do
    children = [
      {Phoenix.PubSub, name: TestServer.PubSub},
      TestServer.Joins,
      {PhoenixDurableChannel.Stream.Memory, kept: 2000, forget_after: 2000},
      {PhoenixDurableChannel.Stream.DurableStreams, keep_offsets_for: 2000},
      {S2,
       name: TestServer.S2,
       basin: "phoenix-durable-channel",
       basin_endpoint: "http://127.0.0.1:4401"},
      {Mongo,
       name: TestServer.Mongo, url: "mongodb://127.0.0.1:4403/phoenix_durable_channel_test"},
      {Task, &PhoenixDurableChannel.Stream.Mongo.create_indexes/0},
      TestServer.Presence,
      TestServer.Endpoint
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: TestServer.Supervisor)
  end
end

defmodule TestServer.Joins do
  use Agent

  def start_link(_), do: Agent.start_link(fn -> %{} end, name: __MODULE__)

  def increment(topic),
    do:
      Agent.get_and_update(
        __MODULE__,
        &Map.get_and_update(&1, topic, fn count -> {(count || 0) + 1, (count || 0) + 1} end)
      )

  def count(topic), do: Agent.get(__MODULE__, &Map.get(&1, topic, 0))

  def first_time?(key), do: increment(key) == 1
end

defmodule TestServer.Presence do
  use Phoenix.Presence, otp_app: :test_server, pubsub_server: TestServer.PubSub
end

defmodule TestServer.Endpoint do
  use Phoenix.Endpoint, otp_app: :test_server

  @options [
    check_origin: false,
    connect_info: [:auth_token],
    serializer: [
      {Phoenix.Socket.V1.JSONSerializer, "~> 1.0.0"},
      {PhoenixDurableChannel.Serializer, "~> 2.0.0"}
    ]
  ]

  socket("/socket", TestServer.UserSocket,
    auth_token: true,
    websocket: [timeout: 45_000] ++ @options,
    longpoll: [window_ms: 1_000] ++ @options
  )

  socket("/idle_socket", TestServer.UserSocket,
    websocket: [timeout: 1_000] ++ @options,
    longpoll: false
  )

  socket("/longpoll_only", TestServer.UserSocket, websocket: false, longpoll: @options)

  socket("/hang", TestServer.UserSocket,
    websocket: false,
    longpoll: [window_ms: 1_000] ++ @options
  )

  plug(:durable_streams)
  plug(:health)

  def durable_streams(%{path_info: ["durable-streams" | _path]} = connection, _options),
    do:
      connection
      |> Plug.Conn.fetch_query_params()
      |> Plug.Conn.read_body(length: :infinity)
      |> forward_to_durable_streams()

  def durable_streams(connection, _options), do: connection

  def forward_to_durable_streams({:ok, body, %{path_info: [_ | path]} = connection}) do
    if connection.method == "GET",
      do:
        TestServer.Joins.increment(
          "durable_streams_read:" <>
            URI.decode_www_form(List.last(path)) <> "@" <> connection.query_params["offset"]
        )

    response =
      Req.request!(
        method: connection.method,
        url: "http://127.0.0.1:4402/" <> Enum.join(path, "/") <> "?" <> connection.query_string,
        headers:
          Enum.reject(connection.req_headers, fn {name, _value} ->
            name in ["host", "content-length"]
          end),
        body: body,
        decode_body: false,
        compressed: false,
        redirect: false,
        retry: false
      )

    response.headers
    |> Enum.reject(fn {name, _values} -> name in ["content-length", "transfer-encoding"] end)
    |> Enum.reduce(connection, fn {name, values}, connection ->
      Plug.Conn.put_resp_header(connection, name, Enum.join(values, ", "))
    end)
    |> Plug.Conn.send_resp(response.status, response.body)
    |> Plug.Conn.halt()
  end

  def health(%{request_path: "/hang/websocket"}, _options), do: Process.sleep(:infinity)

  def health(connection, _options),
    do: connection |> Plug.Conn.send_resp(200, "ok") |> Plug.Conn.halt()
end

defmodule TestServer.UserSocket do
  use Phoenix.Socket

  channel("room:*", TestServer.RoomChannel)
  channel("presence:*", TestServer.PresenceChannel)
  channel("durable:*", TestServer.DurableChannel)
  channel("upgrading:*", TestServer.UpgradingChannel)

  @impl true
  def connect(%{"token" => "bad"}, _socket, _connect_info), do: :error

  def connect(params, socket, connect_info),
    do:
      {:ok,
       socket
       |> assign(:id, params["id"])
       |> assign(:auth_token, connect_info[:auth_token])}

  @impl true
  def id(%{assigns: %{id: nil}}), do: nil
  def id(%{assigns: %{id: id}}), do: "user_socket:" <> id
end

defmodule TestServer.RoomChannel do
  use Phoenix.Channel

  @impl true
  def join("room:deny", _params, _socket), do: {:error, %{reason: "denied"}}

  def join("room:slow" <> _ = topic, %{"sleep" => sleep} = params, socket) do
    Process.sleep(sleep)
    {:ok, %{joins: TestServer.Joins.increment(topic), params: params}, socket}
  end

  def join(topic, params, socket) do
    {:ok, %{joins: TestServer.Joins.increment(topic), params: params}, socket}
  end

  @impl true
  def handle_in("echo", payload, socket), do: {:reply, {:ok, payload}, socket}
  def handle_in("error", payload, socket), do: {:reply, {:error, payload}, socket}
  def handle_in("noreply", _payload, socket), do: {:noreply, socket}

  def handle_in("auth_token", _payload, socket),
    do: {:reply, {:ok, %{auth_token: socket.assigns.auth_token}}, socket}

  def handle_in("joins", _payload, socket),
    do: {:reply, {:ok, %{joins: TestServer.Joins.count(socket.topic)}}, socket}

  def handle_in("count", %{"key" => key}, socket),
    do: {:reply, {:ok, %{count: TestServer.Joins.count(key)}}, socket}

  def handle_in("upgrade", %{"topic" => topic}, socket) do
    TestServer.Joins.increment("upgraded:" <> topic)
    {:reply, :ok, socket}
  end

  def handle_in("slow", %{"sleep" => sleep} = payload, socket) do
    Process.sleep(sleep)
    {:reply, {:ok, payload}, socket}
  end

  def handle_in("broadcast", payload, socket) do
    broadcast!(socket, "broadcasted", payload)
    {:reply, :ok, socket}
  end

  def handle_in("push_to_self", payload, socket) do
    push(socket, "pushed", payload)
    {:reply, :ok, socket}
  end

  def handle_in("crash", _payload, _socket), do: raise("crash requested by the test")
  def handle_in("stop_channel", _payload, socket), do: {:stop, :normal, socket}

  def handle_in("drain", _payload, socket) do
    send(socket.transport_pid, :socket_drain)
    {:noreply, socket}
  end

  def handle_in("kill_transport", _payload, socket) do
    Process.exit(socket.transport_pid, :kill)
    {:noreply, socket}
  end

  def handle_in("echo_binary", {:binary, data}, socket),
    do: {:reply, {:ok, {:binary, data}}, socket}

  def handle_in("binary_size", {:binary, data}, socket),
    do: {:reply, {:ok, %{size: byte_size(data)}}, socket}

  def handle_in("push_binary", _payload, socket) do
    push(socket, "binary", {:binary, <<1, 2, 3>>})
    {:reply, :ok, socket}
  end

  def handle_in("broadcast_binary", _payload, socket) do
    broadcast!(socket, "binary_broadcast", {:binary, <<4, 5, 6>>})
    {:reply, :ok, socket}
  end

  def handle_in("disconnect_socket", _payload, socket) do
    TestServer.Endpoint.broadcast(socket.id, "disconnect", %{})
    {:noreply, socket}
  end
end

defmodule TestServer.PresenceChannel do
  use Phoenix.Channel
  alias TestServer.Presence

  @impl true
  def join(_topic, %{"name" => name}, socket) do
    send(self(), :after_join)
    {:ok, assign(socket, :name, name)}
  end

  @impl true
  def handle_info(:after_join, socket) do
    {:ok, _} = Presence.track(socket, socket.assigns.name, %{name: socket.assigns.name})
    push(socket, "presence_state", Presence.list(socket))
    {:noreply, socket}
  end

  @impl true
  def handle_in("update", %{"status" => status}, socket) do
    {:ok, _} =
      Presence.update(socket, socket.assigns.name, %{name: socket.assigns.name, status: status})

    {:reply, :ok, socket}
  end
end

defmodule TestServer.SlowMemory do
  @behaviour PhoenixDurableChannel.Stream
  @behaviour PhoenixDurableChannel.Stream.ThroughChannel
  alias PhoenixDurableChannel.Stream.Memory

  @impl true
  def append(socket, event, payload, received),
    do: Memory.append(socket, event, payload, received)

  @impl true
  def received(socket), do: Memory.received(socket)

  @impl true
  def reader(socket), do: Memory.reader(socket)

  @impl true
  def epoch(socket), do: Memory.epoch(socket)

  @impl true
  def since(socket, number, limit) do
    if TestServer.Joins.increment("read:" <> socket.topic) == 1, do: Process.sleep(300)
    Memory.since(socket, number, limit)
  end
end

defmodule TestServer.Unreadable do
  @behaviour PhoenixDurableChannel.Stream
  alias PhoenixDurableChannel.Stream.Memory

  @impl true
  def append(socket, event, payload, received),
    do: Memory.append(socket, event, payload, received)

  @impl true
  def received(socket), do: Memory.received(socket)

  @impl true
  def reader(socket), do: Memory.reader(socket)
end

defmodule TestServer.Elsewhere do
  @behaviour PhoenixDurableChannel.Stream

  @impl true
  def append(socket, event, payload, received),
    do: PhoenixDurableChannel.Stream.S2.append(socket, event, payload, received)

  @impl true
  def received(socket), do: PhoenixDurableChannel.Stream.S2.received(socket)

  @impl true
  def reader(socket),
    do: %{
      type: "elsewhere",
      stream: socket.topic,
      last: PhoenixDurableChannel.Stream.S2.last(socket.topic)
    }
end

defmodule TestServer.ExpiringDurableStreams do
  @behaviour PhoenixDurableChannel.Stream
  alias PhoenixDurableChannel.Stream.DurableStreams

  @impl true
  def append(socket, event, payload, received),
    do: DurableStreams.append(socket, event, payload, received)

  @impl true
  def received(socket), do: DurableStreams.received(socket)

  @impl true
  def reader(socket) do
    reader = DurableStreams.reader(socket)

    case TestServer.Joins.increment("reader:" <> socket.topic) do
      calls when calls <= 2 ->
        %{reader | token: DurableStreams.token("read", socket.topic, -60)}

      3 ->
        Process.sleep(900)
        reader

      _calls ->
        reader
    end
  end
end

defmodule TestServer.UpgradingChannel do
  use Phoenix.Channel

  @impl true
  def join(topic, params, socket) do
    if TestServer.Joins.count("upgraded:" <> topic) == 0 do
      TestServer.Joins.increment("old_joins:" <> topic)
      {:ok, %{}, assign(socket, :old, true)}
    else
      TestServer.DurableChannel.join(topic, params, socket)
    end
  end

  @impl true
  def handle_in(event, _payload, %{assigns: %{old: true}}),
    do: raise("a deploy from before PhoenixDurableChannel has no #{event}")

  def handle_in(event, payload, socket),
    do: TestServer.DurableChannel.handle_in(event, payload, socket)

  @impl true
  defdelegate handle_info(message, socket), to: TestServer.DurableChannel

  @impl true
  def terminate(_reason, socket) do
    if socket.assigns[:old], do: TestServer.Joins.increment("old_left:" <> socket.topic)
    :ok
  end
end

defmodule TestServer.DurableChannel do
  use Phoenix.Channel

  use PhoenixDurableChannel.Channel, stream: PhoenixDurableChannel.Stream.Memory

  alias PhoenixDurableChannel.Stream.Memory
  alias PhoenixDurableChannel.Stream.Mongo

  @stores %{
    "memory" => Memory,
    "mongo" => Mongo,
    "s2" => PhoenixDurableChannel.Stream.S2,
    "durable-streams" => PhoenixDurableChannel.Stream.DurableStreams
  }

  @streams Map.merge(@stores, %{
             "durable-streams-expiring" => TestServer.ExpiringDurableStreams,
             "elsewhere" => TestServer.Elsewhere,
             "slow" => TestServer.SlowMemory,
             "unreadable" => TestServer.Unreadable
           })

  def durable_stream(socket), do: Map.get(@streams, socket.assigns[:stream], Memory)

  @impl true
  def join(topic, %{"refuse" => true}, _socket) do
    TestServer.Joins.increment("refused:" <> topic)
    {:error, %{reason: "refused"}}
  end

  def join(_topic, params, socket) do
    if params["greet"], do: send(self(), :greet)
    if params["busy_after_join"], do: send(self(), :busy)
    if params["not_durable"], do: send(self(), :not_durable)

    {:ok, assign(socket, stream: params["stream"])}
  end

  @impl true
  def handle_info(:busy, socket) do
    Process.sleep(300)
    {:noreply, socket}
  end

  def handle_info(:not_durable, socket), do: {:noreply, assign(socket, phx_durable: false)}

  def handle_info(:forget, socket) do
    :ets.insert(Memory, {socket.topic, %{Memory.kept(socket.topic) | messages: []}})
    {:noreply, socket}
  end

  def handle_info(:greet, socket) do
    push_durable(socket, "update", %{body: "welcome"})
    {:noreply, socket}
  end

  @impl true
  def handle_in("publish", %{"body" => body}, socket) do
    push_durable(socket, "update", %{body: body})
    {:reply, :ok, socket}
  end

  def handle_in("publish_event", %{"event" => event}, socket) do
    push_durable(socket, event, %{})
    {:reply, :ok, socket}
  end

  def handle_in("append_unpushed", %{"event" => event, "payload" => payload}, socket) do
    durable_stream(socket).append(socket, event, payload, socket.assigns.phx_received)
    push_durable(socket, "update", %{body: "after"})
    {:reply, :ok, socket}
  end

  def handle_in("publish_large", _payload, socket) do
    push_durable(socket, "large", %{body: String.duplicate("x", 600_000)})
    {:reply, :ok, socket}
  end

  def handle_in("trim_nothing", _payload, socket) do
    trim = %S2.V1.AppendRecord{
      headers: [%S2.V1.Header{name: "", value: "trim"}],
      body: <<0::64>>
    }

    case S2.append(PhoenixDurableChannel.Stream.S2.config()[:name], socket.topic, [trim]) do
      {:ok, _ack} -> {:reply, :ok, socket}
    end
  end

  def handle_in("publish_silently", %{"body" => body}, socket) do
    number =
      durable_stream(socket).append(socket, "update", %{body: body}, socket.assigns.phx_received)

    {:reply, {:ok, %{number: number}}, socket}
  end

  def handle_in("forget", _payload, socket) do
    :ets.insert(Memory, {socket.topic, %{Memory.kept(socket.topic) | messages: []}})
    {:reply, :ok, socket}
  end

  def handle_in("reset", _payload, socket) do
    :ets.insert(Memory, {{:before_reset, socket.topic}, Memory.kept(socket.topic)})
    :ets.delete(Memory, socket.topic)
    {:reply, :ok, socket}
  end

  def handle_in("restore", _payload, socket) do
    :ets.insert(
      Memory,
      {socket.topic, :ets.lookup_element(Memory, {:before_reset, socket.topic}, 2)}
    )

    {:reply, :ok, socket}
  end

  def handle_in("fire", payload, socket) do
    push(socket, "fired", payload)
    {:reply, :ok, socket}
  end

  def handle_in("durable", _payload, socket),
    do: {:reply, {:ok, %{durable: Map.get(socket.assigns, :phx_durable, false)}}, socket}

  def handle_in("ledger", _payload, socket) do
    received = durable_stream(socket).received(socket)
    sent = durable_stream(socket).reader(socket).last
    {:reply, {:ok, %{received: received, sent: sent}}, socket}
  end

  def handle_in("counts", %{"topic" => topic}, socket),
    do:
      {:reply,
       {:ok,
        %{
          reads: TestServer.Joins.count("read:" <> topic),
          refused: TestServer.Joins.count("refused:" <> topic)
        }}, socket}

  def handle_in("handled", _payload, socket),
    do: {:reply, {:ok, %{handled: TestServer.Joins.count("handled:" <> socket.topic)}}, socket}

  def handle_in("kill_transport", _payload, socket) do
    Process.exit(socket.transport_pid, :kill)
    {:noreply, socket}
  end

  def handle_in("action", %{"body" => "refuse"}, socket),
    do: {:reply, {:error, %{reason: "refused"}}, socket}

  def handle_in("action", %{"body" => "quiet"}, socket), do: {:noreply, socket}

  def handle_in("action", %{"body" => "stop"}, socket),
    do: {:stop, :normal, {:ok, %{stopped: true}}, socket}

  def handle_in("action", %{"body" => "stop_quietly"}, socket), do: {:stop, :normal, socket}
  def handle_in("action", %{"body" => "stop_ok"}, socket), do: {:stop, :normal, :ok, socket}
  def handle_in("action", %{"body" => "hibernate"}, socket), do: {:noreply, socket, :hibernate}
  def handle_in("action", %{"body" => "ok"}, socket), do: {:reply, :ok, socket}
  def handle_in("action", %{"body" => "crash"}, _socket), do: raise("crash requested by the test")

  def handle_in("action", %{"body" => "lose_reply"}, socket) do
    Process.exit(socket.transport_pid, :kill)
    {:noreply, socket}
  end

  def handle_in("action", %{"body" => "lose_and_forget_reply"}, socket) do
    send(self(), :forget)
    Process.exit(socket.transport_pid, :kill)
    {:reply, :ok, socket}
  end

  def handle_in("action", %{"body" => "skip_and_forget_reply"}, socket) do
    durable_stream(socket).append(
      socket,
      "update",
      %{body: "unseen"},
      socket.assigns.phx_received
    )

    send(self(), :forget)
    {:reply, :ok, socket}
  end

  def handle_in("action", %{"body" => "busy_once"} = action, socket) do
    if TestServer.Joins.first_time?("busy_once:" <> socket.topic),
      do: {:retry, "busy", socket},
      else: {:reply, {:ok, action}, socket}
  end

  def handle_in("action", %{"body" => "refuse_and_lose"}, socket) do
    Process.exit(socket.transport_pid, :kill)
    {:reply, {:error, %{reason: "refused"}}, socket}
  end

  def handle_in("action", %{"body" => "slow"} = action, socket) do
    Process.sleep(500)
    TestServer.Joins.increment("handled:" <> socket.topic)
    {:reply, {:ok, action}, socket}
  end

  def handle_in("publish_many_silently", %{"count" => count}, socket) do
    Enum.each(1..count, fn number ->
      durable_stream(socket).append(
        socket,
        "update",
        %{body: number},
        socket.assigns.phx_received
      )
    end)

    {:reply, :ok, socket}
  end

  def handle_in("action", %{"body" => "number"}, socket),
    do: {:reply, {:ok, %{number: socket.assigns.phx_number}}, socket}

  def handle_in("action", action, socket), do: {:reply, {:ok, action}, socket}
end
