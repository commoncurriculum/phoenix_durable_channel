defmodule TestServer.Application do
  use Application

  def start(_type, _args) do
    children = [
      {Phoenix.PubSub, name: TestServer.PubSub},
      TestServer.Joins,
      TestServer.Ledger,
      PhoenixDurableChannel.Stream.Memory,
      PhoenixDurableChannel.Stream.DurableStreams,
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
  # Counts joins per topic, so a test can see how many times the client joined.
  use Agent

  def start_link(_), do: Agent.start_link(fn -> %{} end, name: __MODULE__)

  def increment(topic),
    do:
      Agent.get_and_update(
        __MODULE__,
        &Map.get_and_update(&1, topic, fn count -> {(count || 0) + 1, (count || 0) + 1} end)
      )

  def count(topic), do: Agent.get(__MODULE__, &Map.get(&1, topic, 0))
end

defmodule TestServer.Presence do
  use Phoenix.Presence, otp_app: :test_server, pubsub_server: TestServer.PubSub
end

defmodule TestServer.Endpoint do
  use Phoenix.Endpoint, otp_app: :test_server

  # The tests run in Chromium on another port, so origins are not checked. Every socket uses
  # the library's serializer, so plain frames are tested through it too.
  @options [check_origin: false, serializer: [{PhoenixDurableChannel.Serializer, "~> 2.0.0"}]]

  socket("/socket", TestServer.UserSocket,
    websocket: [timeout: 45_000] ++ @options,
    # A poll is held at most this long, so a poll on a killed session ends quickly.
    longpoll: [window_ms: 1_000] ++ @options
  )

  # Cowboy closes an idle WebSocket with code 1000; this socket goes idle after 1 s.
  socket("/idle_socket", TestServer.UserSocket,
    websocket: [timeout: 1_000] ++ @options,
    longpoll: false
  )

  # Refuses WebSocket, so a client has to fall back to LongPoll.
  socket("/longpoll_only", TestServer.UserSocket, websocket: false, longpoll: @options)

  # Has LongPoll, and a WebSocket path that never answers (see `health`).
  socket("/hang", TestServer.UserSocket,
    websocket: false,
    longpoll: [window_ms: 1_000] ++ @options
  )

  # A plain HTTP 200 the tests use to tell the server is up.
  plug(:health)

  def health(%{request_path: "/hang/websocket"}, _opts), do: Process.sleep(:infinity)
  def health(conn, _opts), do: conn |> Plug.Conn.send_resp(200, "ok") |> Plug.Conn.halt()
end

defmodule TestServer.UserSocket do
  use Phoenix.Socket

  channel("room:*", TestServer.RoomChannel)
  channel("presence:*", TestServer.PresenceChannel)
  channel("durable:*", TestServer.DurableChannel)
  channel("upgrading:*", TestServer.UpgradingChannel)

  @impl true
  def connect(%{"token" => "bad"}, _socket, _connect_info), do: :error
  def connect(params, socket, _connect_info), do: {:ok, assign(socket, :id, params["id"])}

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

  # Phoenix's drainer sends this; the socket closes with code 1012.
  def handle_in("drain", _payload, socket) do
    send(socket.transport_pid, :socket_drain)
    {:noreply, socket}
  end

  # Kills the process behind this connection: the WebSocket process, or the LongPoll
  # session, as a crashed node would.
  def handle_in("kill_transport", _payload, socket) do
    Process.exit(socket.transport_pid, :kill)
    {:noreply, socket}
  end

  def handle_in("echo_binary", {:binary, data}, socket),
    do: {:reply, {:ok, {:binary, data}}, socket}

  # LongPoll carries client binaries as base64 but cannot carry server binaries back.
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

  # The "disconnect" broadcast Phoenix documents for logging a user out of every socket.
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

defmodule TestServer.Ledger do
  # A PhoenixDurableChannel.Ledger and PhoenixDurableChannel.Stream kept in memory, per topic, that tests
  # can make forget, reset or answer slowly.
  @behaviour PhoenixDurableChannel.Ledger
  @behaviour PhoenixDurableChannel.Stream
  @behaviour PhoenixDurableChannel.Stream.ThroughChannel
  use Agent

  @empty %{
    sent: [],
    received: 0,
    replies: %{},
    forgotten: 0,
    used: MapSet.new(),
    epoch: "first",
    handled: 0,
    previous: nil
  }

  def start_link(_), do: Agent.start_link(fn -> %{} end, name: __MODULE__)

  def update(topic, function) do
    Agent.get_and_update(__MODULE__, fn state ->
      {reply, ledger} = function.(Map.get(state, topic, @empty))
      {reply, Map.put(state, topic, ledger)}
    end)
  end

  def get(topic), do: Agent.get(__MODULE__, &Map.get(&1, topic, @empty))

  @impl true
  def append(socket, event, payload) do
    update(socket.topic, fn ledger ->
      number = length(ledger.sent) + 1
      {number, %{ledger | sent: ledger.sent ++ [{number, event, payload}]}}
    end)
  end

  # On a "durable:slow_read…" topic the first read answers late, so the client can leave first.
  @impl true
  def since(socket, number, limit) do
    if String.starts_with?(socket.topic, "durable:slow_read") and
         first_time?(socket.topic, :slow_read),
       do: Process.sleep(300)

    ledger = get(socket.topic)
    latest = length(ledger.sent)
    sent = min(latest, number + limit)

    messages =
      Enum.filter(ledger.sent, fn {kept, _, _} ->
        kept > number and kept <= sent and kept > ledger.forgotten
      end)

    %{messages: messages, sent: sent, latest: latest, epoch: ledger.epoch}
  end

  # On a "durable:slow_sync…" topic the first sync answers late, so the client's sync times out.
  @impl true
  def received(socket) do
    if String.starts_with?(socket.topic, "durable:slow_sync") and
         first_time?(socket.topic, :slow_sync),
       do: Process.sleep(300)

    get(socket.topic).received
  end

  @impl true
  def put_received(socket, number, reply) do
    update(socket.topic, fn ledger ->
      {:ok,
       %{
         ledger
         | received: max(ledger.received, number),
           replies: Map.put(ledger.replies, number, reply)
       }}
    end)
  end

  @impl true
  def reply(socket, number), do: Map.get(get(socket.topic).replies, number)

  @impl true
  def reader(socket) do
    ledger = get(socket.topic)
    %{type: "phoenix", last: length(ledger.sent), epoch: ledger.epoch}
  end

  # Drops every message kept so far, as a ledger that prunes old entries would.
  def forget(topic),
    do: update(topic, fn ledger -> {:ok, %{ledger | forgotten: length(ledger.sent)}} end)

  # Loses everything and numbers from 1 again, as another node's memory, or this one's after
  # a restart, would.
  def reset(topic) do
    epoch = Integer.to_string(System.unique_integer([:positive]))
    update(topic, fn ledger -> {:ok, %{@empty | epoch: epoch, previous: ledger}} end)
  end

  # Back to what was kept before the reset, as the first node's memory.
  def restore(topic), do: update(topic, fn ledger -> {:ok, ledger.previous} end)

  # Counts the "slow" actions handled, so a test can tell one that ran twice.
  def handled(topic),
    do: update(topic, fn ledger -> {:ok, %{ledger | handled: ledger.handled + 1}} end)

  # True the first time it is asked for a topic and key only.
  def first_time?(topic, key) do
    update(topic, fn ledger ->
      {not MapSet.member?(ledger.used, key), %{ledger | used: MapSet.put(ledger.used, key)}}
    end)
  end
end

defmodule TestServer.Elsewhere do
  # A store the client has no built-in reader for: S2, published as type "elsewhere".
  @behaviour PhoenixDurableChannel.Stream

  @impl true
  def append(socket, event, payload),
    do: PhoenixDurableChannel.Stream.S2.append(socket, event, payload)

  @impl true
  def reader(socket),
    do: %{
      type: "elsewhere",
      stream: socket.topic,
      last: PhoenixDurableChannel.Stream.S2.last(socket.topic)
    }
end

defmodule TestServer.ExpiringDurableStreams do
  # Durable Streams whose join gives a read token that already expired. The first fresh
  # one is slow to come, so the client's request for it times out.
  @behaviour PhoenixDurableChannel.Stream
  alias PhoenixDurableChannel.Stream.DurableStreams

  @impl true
  def append(socket, event, payload), do: DurableStreams.append(socket, event, payload)

  @impl true
  def reader(socket) do
    reader = DurableStreams.reader(socket)

    cond do
      TestServer.Ledger.first_time?(socket.topic, :join) ->
        %{reader | token: DurableStreams.token("read", socket.topic, -60)}

      TestServer.Ledger.first_time?(socket.topic, :slow_reader) ->
        Process.sleep(900)
        reader

      true ->
        reader
    end
  end
end

defmodule TestServer.UpgradingChannel do
  # A topic served as a deploy from before PhoenixDurableChannel would serve it, joined without a
  # reader, until a room's "upgrade" says the deploy is done; then as TestServer.DurableChannel.
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
  use PhoenixDurableChannel.Channel, ledger: TestServer.Ledger, stream: TestServer.Ledger
  alias TestServer.Ledger

  @stores %{
    "memory" => PhoenixDurableChannel.Stream.Memory,
    "mongo" => PhoenixDurableChannel.Stream.Mongo,
    "s2" => PhoenixDurableChannel.Stream.S2,
    "durable-streams" => PhoenixDurableChannel.Stream.DurableStreams
  }

  @streams Map.merge(@stores, %{
             "durable-streams-expiring" => TestServer.ExpiringDurableStreams,
             "elsewhere" => TestServer.Elsewhere
           })

  # The stream the join's `stream` param names: TestServer.Ledger by default.
  def durable_stream(socket), do: Map.get(@streams, socket.assigns[:stream], Ledger)

  # The ledger the join's `ledger` param names: TestServer.Ledger by default.
  def durable_ledger(socket), do: Map.get(@stores, socket.assigns[:ledger], Ledger)

  @impl true
  def join(_topic, params, socket) do
    if params["greet"], do: send(self(), :greet)
    {:ok, assign(socket, stream: params["stream"], ledger: params["ledger"])}
  end

  # Sent right after the join reply, so it reaches the client before its first read's reply.
  @impl true
  def handle_info(:greet, socket) do
    push_durable(socket, "update", %{body: "welcome"})
    {:noreply, socket}
  end

  @impl true
  def handle_in("publish", %{"body" => body}, socket) do
    push_durable(socket, "update", %{body: body})
    {:reply, :ok, socket}
  end

  # Kept and numbered, but not sent: the client only gets it by asking.
  def handle_in("publish_silently", %{"body" => body}, socket) do
    {:reply, {:ok, %{number: Ledger.append(socket, "update", %{body: body})}}, socket}
  end

  def handle_in("forget", _payload, socket) do
    Ledger.forget(socket.topic)
    {:reply, :ok, socket}
  end

  def handle_in("reset", _payload, socket) do
    Ledger.reset(socket.topic)
    {:reply, :ok, socket}
  end

  def handle_in("restore", _payload, socket) do
    Ledger.restore(socket.topic)
    {:reply, :ok, socket}
  end

  def handle_in("fire", payload, socket) do
    push(socket, "fired", payload)
    {:reply, :ok, socket}
  end

  def handle_in("durable", _payload, socket),
    do: {:reply, {:ok, %{durable: socket.assigns.phx_durable}}, socket}

  def handle_in("ledger", _payload, socket) do
    ledger = Ledger.get(socket.topic)
    {:reply, {:ok, %{received: ledger.received, sent: length(ledger.sent)}}, socket}
  end

  def handle_in("handled", _payload, socket) do
    {:reply, {:ok, %{handled: Ledger.get(socket.topic).handled}}, socket}
  end

  # Kills this connection, as a crashed node would.
  def handle_in("kill_transport", _payload, socket) do
    Process.exit(socket.transport_pid, :kill)
    {:noreply, socket}
  end

  def handle_in("action", %{"body" => "refuse"}, socket),
    do: {:reply, {:error, %{reason: "refused"}}, socket}

  def handle_in("action", %{"body" => "quiet"}, socket), do: {:noreply, socket}
  def handle_in("action", %{"body" => "crash"}, _socket), do: raise("crash requested by the test")

  # Handled, then the connection dies before the reply goes out. The resend is historic.
  def handle_in("action", %{"body" => "lose_reply"}, socket) do
    Process.exit(socket.transport_pid, :kill)
    {:noreply, socket}
  end

  # Not taken the first time, as with a database that is briefly down.
  def handle_in("action", %{"body" => "busy_once"} = action, socket) do
    if Ledger.first_time?(socket.topic, :busy_once),
      do: {:retry, "busy", socket},
      else: {:reply, {:ok, action}, socket}
  end

  # Refused, then the connection dies before the refusal goes out. The resend gets it.
  def handle_in("action", %{"body" => "refuse_and_lose"}, socket) do
    Process.exit(socket.transport_pid, :kill)
    {:reply, {:error, %{reason: "refused"}}, socket}
  end

  # Slow, so the client's connection can drop while this process still runs it.
  def handle_in("action", %{"body" => "slow"} = action, socket) do
    Process.sleep(500)
    Ledger.handled(socket.topic)
    {:reply, {:ok, action}, socket}
  end

  def handle_in("publish_many_silently", %{"count" => count}, socket) do
    Enum.each(1..count, fn number -> Ledger.append(socket, "update", %{body: number}) end)
    {:reply, :ok, socket}
  end

  def handle_in("action", %{"body" => "number"}, socket),
    do: {:reply, {:ok, %{number: socket.assigns.phx_number}}, socket}

  def handle_in("action", action, socket), do: {:reply, {:ok, action}, socket}
end
