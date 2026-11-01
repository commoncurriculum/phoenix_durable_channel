defmodule PhoenixDurableChannel.Channel do
  @moduledoc """
  A `Phoenix.Channel` that also speaks durable messages, both ways.

      use Phoenix.Channel
      use PhoenixDurableChannel.Channel, ledger: PhoenixDurableChannel.Stream.S2, stream: PhoenixDurableChannel.Stream.S2

  It comes after `use Phoenix.Channel` (or an app macro that uses it), so it adds to the
  channel instead of defining a second one. `ledger:` and `stream:` are read on every call,
  so they can come from the app's runtime config; a channel can also define
  `durable_ledger/1` or `durable_stream/1` to choose one per socket. Each store the library
  ships, `PhoenixDurableChannel.Stream.Memory`, `Mongo`, `S2` and `DurableStreams`, is both.

  `push/3` stays fire and forget. `push_durable/3` appends the message to the stream, a
  `PhoenixDurableChannel.Stream`. The join reply's `phx_reader` tells the client where to read it.
  A stream that is a `PhoenixDurableChannel.Stream.ThroughChannel` is read through this channel: it
  pushes each message with its number, and answers the client's `phx_read` with what the
  stream kept after the client's last. Any other is read straight from its store, such as
  S2, and Phoenix never carries the messages. `phx_reader` asks for the reader again, for a
  client whose read token expired.

  A durable message from the client reaches `handle_in/3` in order, with its plain payload;
  `{:noreply, socket}` is answered `:ok`. The ledger, a `PhoenixDurableChannel.Ledger`, records the
  number and the reply after `handle_in/3` returns, and a resend after a lost reply gets that
  same reply. The client's `phx_sync` asks for the last number the ledger recorded.

  A topic names one client, and a durable join makes its process the topic's only one: a
  process from the client's earlier connection can outlive that connection and still run
  the messages it already received, so the join waits for it to finish them and stop. A
  message runs once, except when its process dies between `handle_in/3` and the ledger's
  record: the client sends it again and it runs again, so a handler that must not run twice
  checks for its own earlier effect.

  `handle_in/3` may return `{:retry, reason, socket}` for a durable message it cannot take
  now (a database that is down). It is not recorded, the client's later messages are
  refused as not taken too, and the client sends them all again, in order, after a backoff.

  A `handle_in/3` that raises, throws or exits for a durable message is durable, and the
  message is answered and recorded as `{:error, %{reason: "crashed"}}`, so it is not run
  again. Return `{:retry, reason, socket}` for a failure worth trying again.

  Durable payloads are maps; binary payloads are fire and forget only.
  """

  require Logger

  # How long a durable join waits for the topic's earlier process before killing it.
  @replace_timeout 5_000
  # The most messages a `phx_read` loads from the stream at once.
  @page 500

  defmacro __using__(options) do
    quote do
      @before_compile PhoenixDurableChannel.Channel

      @doc "The `PhoenixDurableChannel.Ledger` that records the client's durable messages."
      def durable_ledger(_socket), do: unquote(Keyword.fetch!(options, :ledger))

      @doc "The `PhoenixDurableChannel.Stream` this channel's durable messages go to."
      def durable_stream(_socket), do: unquote(Keyword.fetch!(options, :stream))

      defoverridable durable_ledger: 1, durable_stream: 1

      def push_durable(socket, event, payload) when is_map(payload),
        do:
          PhoenixDurableChannel.Channel.push_durable(
            durable_stream(socket),
            socket,
            event,
            payload
          )
    end
  end

  defmacro __before_compile__(env) do
    handle_info =
      if Module.defines?(env.module, {:handle_info, 2}) do
        quote do
          defoverridable handle_info: 2

          def handle_info(:phx_replaced, socket),
            do: PhoenixDurableChannel.Channel.replaced(socket)

          def handle_info(message, socket), do: super(message, socket)
        end
      else
        quote do
          def handle_info(:phx_replaced, socket),
            do: PhoenixDurableChannel.Channel.replaced(socket)
        end
      end

    quote do
      defoverridable join: 3, handle_in: 3

      def join(topic, params, socket),
        do:
          PhoenixDurableChannel.Channel.join(
            super(topic, params, socket),
            params,
            &durable_stream/1
          )

      def handle_in("phx_sync", _payload, socket),
        do: PhoenixDurableChannel.Channel.sync(durable_ledger(socket), socket)

      def handle_in("phx_read", %{"after" => last}, socket),
        do: PhoenixDurableChannel.Channel.read(durable_stream(socket), last, socket)

      def handle_in("phx_reader", _payload, socket),
        do: {:reply, {:ok, durable_stream(socket).reader(socket)}, socket}

      def handle_in(event, {:durable, number, payload}, socket) do
        PhoenixDurableChannel.Channel.handle_durable(
          durable_ledger(socket),
          number,
          socket,
          fn socket ->
            super(event, payload, socket)
          end
        )
      end

      def handle_in(event, payload, socket), do: super(event, payload, socket)

      unquote(handle_info)
    end
  end

  @doc """
  Assigns `:phx_durable`: whether the client was opened with `durable`, so it reads the
  durable messages. When it was, makes this process the topic's only one, and adds where the
  client reads them, `phx_reader`, to the join's reply.
  """
  def join({:ok, socket}, params, stream_for), do: join({:ok, %{}, socket}, params, stream_for)

  def join({:ok, reply, socket}, %{"phx_durable" => true}, stream_for) when is_map(reply) do
    replace(socket.topic)
    socket = Phoenix.Socket.assign(socket, :phx_durable, true)
    {:ok, Map.put(reply, :phx_reader, stream_for.(socket).reader(socket)), socket}
  end

  def join({:ok, reply, socket}, _params, _stream_for) when is_map(reply),
    do: {:ok, reply, Phoenix.Socket.assign(socket, :phx_durable, false)}

  def join(result, _params, _stream_for), do: result

  @doc """
  Registers the calling process as `topic`'s, on every node, after stopping the one
  registered before it. That one stops once it has run the messages already in its mailbox,
  or is killed after #{@replace_timeout} ms.
  """
  def replace(topic) do
    name = {__MODULE__, topic}

    :global.trans({name, self()}, fn ->
      stop(:global.whereis_name(name))
      :yes = :global.re_register_name(name, self())
    end)
  end

  @doc """
  Stops a process a later durable join replaced. Its client is sent `phx_error`, not only
  `phx_close`, so a client still on this join rejoins: the join that replaced it can be a
  late one from a connection the client already gave up on.
  """
  def replaced(socket) do
    Phoenix.Channel.push(socket, "phx_error", %{})
    {:stop, {:shutdown, :replaced}, socket}
  end

  @doc "Stops a topic's earlier process, and returns once it is gone."
  def stop(:undefined), do: :ok

  def stop(process) do
    monitor = Process.monitor(process)
    send(process, :phx_replaced)

    receive do
      {:DOWN, ^monitor, :process, _process, _reason} -> :ok
    after
      @replace_timeout ->
        Process.exit(process, :kill)

        receive do
          {:DOWN, ^monitor, :process, _process, _reason} -> :ok
        end
    end
  end

  @doc "Appends a message to the stream, and sends it when the client reads through the channel."
  def push_durable(stream, socket, event, payload) do
    number = stream.append(socket, event, payload)

    if through_channel?(stream),
      do: Phoenix.Channel.push(socket, event, {:durable, number, payload})

    :ok
  end

  @doc "Whether the client reads `stream` through the channel."
  def through_channel?(stream) do
    behaviours =
      stream.module_info(:attributes) |> Keyword.get_values(:behaviour) |> Enum.concat()

    PhoenixDurableChannel.Stream.ThroughChannel in behaviours
  end

  @doc "Answers `phx_sync` with the number of the last durable message the client's ledger recorded."
  def sync(ledger, socket), do: {:reply, {:ok, %{received: ledger.received(socket)}}, socket}

  @doc """
  Answers `phx_read`: sends again what the stream kept after `last`, a page at a time, then
  replies with the last number it sent and the stream's epoch. The resends go out before
  the reply, so a number up to `sent` the client still lacks once it has the reply is gone
  from the stream.
  """
  def read(stream, last, socket) do
    page = stream.since(socket, last, @page)

    Enum.each(page.messages, fn {number, event, payload} ->
      Phoenix.Channel.push(socket, event, {:durable, number, payload})
    end)

    if page.sent < page.latest,
      do: read(stream, page.sent, socket),
      else: {:reply, {:ok, %{sent: page.sent, epoch: page.epoch}}, socket}
  end

  @doc """
  Handles the client's durable message `number` in order. `handle_in/3` finds the number in
  `socket.assigns.phx_number`.
  """
  def handle_durable(ledger, number, socket, handle_in) do
    received = ledger.received(socket)

    cond do
      number <= received ->
        {:reply, ledger.reply(socket, number), socket}

      number > received + 1 ->
        {:reply, {:error, %{phx_not_taken: "out_of_order"}}, socket}

      true ->
        case run(handle_in, Phoenix.Socket.assign(socket, :phx_number, number)) do
          {:retry, reason, socket} -> {:reply, {:error, %{phx_not_taken: reason}}, socket}
          {:noreply, socket} -> record(ledger, number, {:ok, %{}}, socket)
          {:reply, :ok, socket} -> record(ledger, number, {:ok, %{}}, socket)
          {:reply, reply, socket} -> record(ledger, number, reply, socket)
        end
    end
  end

  @doc "Records the reply to the client's durable message `number`, and gives it."
  def record(ledger, number, reply, socket) do
    :ok = ledger.put_received(socket, number, reply)
    {:reply, reply, socket}
  end

  @doc """
  Runs a durable message's handler, answering a crash with an error so it is recorded as
  answered. A crash would otherwise come back with every resend, forever.
  """
  def run(handle_in, socket) do
    handle_in.(socket)
  catch
    kind, reason ->
      Logger.error(Exception.format(kind, reason, __STACKTRACE__))
      {:reply, {:error, %{reason: "crashed"}}, socket}
  end
end
