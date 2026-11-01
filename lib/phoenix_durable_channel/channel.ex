defmodule PhoenixDurableChannel.Channel do
  require Logger

  @replace_timeout 5_000
  @page 500

  defmacro __using__(options) do
    quote do
      @before_compile PhoenixDurableChannel.Channel

      def durable_ledger(_socket), do: unquote(Keyword.fetch!(options, :ledger))

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

          def handle_info(message, socket),
            do: PhoenixDurableChannel.Channel.unexpected_message(message, socket)
        end
      end

    defines_handle_in = Module.defines?(env.module, {:handle_in, 3})

    handle_in =
      if defines_handle_in do
        quote do
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
        end
      end

    quote do
      defoverridable join: 3

      if unquote(defines_handle_in), do: defoverridable(handle_in: 3)

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

      unquote(handle_in)

      unquote(handle_info)
    end
  end

  def join({:ok, socket}, %{"phx_durable" => true} = params, stream_for),
    do: join({:ok, %{}, socket}, params, stream_for)

  def join({:ok, reply, socket}, %{"phx_durable" => true}, stream_for) when is_map(reply) do
    replace(socket.topic)
    socket = Phoenix.Socket.assign(socket, :phx_durable, true)
    {:ok, Map.put(reply, :phx_reader, stream_for.(socket).reader(socket)), socket}
  end

  def join(result, _params, _stream_for), do: result

  def replace(topic) do
    name = {__MODULE__, topic}

    :global.trans({name, self()}, fn ->
      stop(:global.whereis_name(name))
      :yes = :global.re_register_name(name, self())
    end)
  end

  def unexpected_message(message, socket) do
    name = Process.info(self(), :registered_name) |> elem(1)
    process = if name == [], do: self(), else: name

    :error_logger.warning_msg(
      ~c"~p ~p received unexpected message in handle_info/2: ~p~n",
      [socket.channel, process, message]
    )

    {:noreply, socket}
  end

  def replaced(socket) do
    Phoenix.Channel.push(socket, "phx_error", %{})
    {:stop, {:shutdown, :replaced}, socket}
  end

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

  def push_durable(stream, socket, event, payload) do
    number = stream.append(socket, event, payload)

    if through_channel?(stream),
      do: Phoenix.Channel.push(socket, event, {:durable, number, payload})

    :ok
  end

  def through_channel?(stream) do
    behaviours =
      stream.module_info(:attributes) |> Keyword.get_values(:behaviour) |> Enum.concat()

    PhoenixDurableChannel.Stream.ThroughChannel in behaviours
  end

  def sync(ledger, socket), do: {:reply, {:ok, %{received: ledger.received(socket)}}, socket}

  def read(stream, last, socket) do
    page = stream.since(socket, last, @page)

    Enum.each(page.messages, fn {number, event, payload} ->
      Phoenix.Channel.push(socket, event, {:durable, number, payload})
    end)

    if page.sent < page.latest,
      do: read(stream, page.sent, socket),
      else: {:reply, {:ok, %{sent: page.sent, epoch: page.epoch}}, socket}
  end

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

  def record(ledger, number, reply, socket) do
    :ok = ledger.put_received(socket, number, reply)
    {:reply, reply, socket}
  end

  def run(handle_in, socket) do
    handle_in.(socket)
  catch
    kind, reason ->
      Logger.error(Exception.format(kind, reason, __STACKTRACE__))
      {:reply, {:error, %{reason: "crashed"}}, socket}
  end
end
