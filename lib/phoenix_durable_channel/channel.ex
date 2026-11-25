defmodule PhoenixDurableChannel.Channel do
  require Logger

  alias Phoenix.Socket

  @replace_timeout 5_000
  @page 500
  @protocol ["phx_sync", "phx_read", "phx_reader"]
  @invalid {:error, %{reason: "invalid"}}

  defmacro __using__(options) do
    stream = Keyword.fetch!(options, :stream)

    quote do
      @before_compile PhoenixDurableChannel.Channel

      @spec durable_stream(Phoenix.Socket.t()) :: module()
      def durable_stream(_socket), do: unquote(stream)

      defoverridable durable_stream: 1

      @spec push_durable(Phoenix.Socket.t(), String.t(), map()) :: :ok
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
    defines_handle_in = Module.defines?(env.module, {:handle_in, 3})
    defines_handle_info = Module.defines?(env.module, {:handle_info, 2})

    app_handle_in =
      if defines_handle_in,
        do: quote(do: fn event, payload, socket -> super(event, payload, socket) end),
        else: nil

    app_handle_info =
      if defines_handle_info,
        do: quote(do: fn message, socket -> super(message, socket) end),
        else: quote(do: &PhoenixDurableChannel.Channel.unexpected_message/2)

    overridable =
      [join: 3] ++
        if(defines_handle_in, do: [handle_in: 3], else: []) ++
        if(defines_handle_info, do: [handle_info: 2], else: [])

    quote do
      defoverridable unquote(overridable)

      def join(topic, params, socket),
        do: PhoenixDurableChannel.Channel.join(params, fn -> super(topic, params, socket) end)

      def handle_in(event, payload, socket),
        do:
          PhoenixDurableChannel.Channel.handle_in(
            __MODULE__,
            event,
            payload,
            socket,
            unquote(app_handle_in)
          )

      def handle_info(message, socket),
        do:
          PhoenixDurableChannel.Channel.handle_info(
            __MODULE__,
            message,
            socket,
            unquote(app_handle_info)
          )
    end
  end

  @doc false
  def handle_in(channel, event, _payload, _socket, nil) when event not in @protocol,
    do: raise(UndefinedFunctionError, module: channel, function: :handle_in, arity: 3)

  def handle_in(channel, event, payload, %{assigns: %{phx_durable: true}} = socket, app),
    do: durable_in(channel, event, payload, socket, app)

  def handle_in(_channel, event, payload, socket, app), do: plain_in(event, payload, socket, app)

  defp durable_in(channel, "phx_sync", _payload, socket, _app),
    do: sync(channel.durable_stream(socket), socket)

  defp durable_in(channel, "phx_read", %{"after" => last}, socket, _app)
       when is_integer(last) and last >= 0,
       do: read(channel.durable_stream(socket), last, socket)

  defp durable_in(channel, "phx_reader", _payload, socket, _app),
    do: {:reply, {:ok, channel.durable_stream(socket).reader(socket)}, socket}

  defp durable_in(channel, event, {:durable, number, payload}, socket, app)
       when is_integer(number) and number > 0,
       do:
         handle_durable(channel.durable_stream(socket), number, socket, &app.(event, payload, &1))

  defp durable_in(_channel, event, payload, socket, app),
    do: plain_in(event, payload, socket, app)

  defp plain_in(event, _payload, socket, _app) when event in @protocol,
    do: {:reply, @invalid, socket}

  defp plain_in(_event, {:durable, _number, _payload}, socket, _app),
    do: {:reply, @invalid, socket}

  defp plain_in(event, payload, socket, app), do: app.(event, payload, socket)

  @doc false
  def handle_info(_channel, :phx_replaced, socket, _app), do: replaced(socket)
  def handle_info(channel, :phx_durable_joined, socket, _app), do: joined(channel, socket)
  def handle_info(_channel, message, socket, app), do: app.(message, socket)

  @doc false
  def join(%{"phx_durable" => true}, join) do
    send(self(), :phx_durable_joined)

    case join.() do
      {:ok, socket} -> durable_join(%{}, socket)
      {:ok, reply, socket} when is_map(reply) -> durable_join(reply, socket)
      result -> result
    end
  end

  def join(_params, join), do: join.()

  defp durable_join(reply, socket) do
    emit(:join, socket)
    {:ok, Map.put(reply, :phx_durable, true), Socket.assign(socket, :phx_durable, true)}
  end

  defp joined(channel, socket) do
    take_over(socket)
    emit(:register, socket)

    socket = Socket.assign(socket, :phx_fence, fence())
    stream = channel.durable_stream(socket)
    reader = stream.reader(socket)
    emit(:reader, socket, %{last: reader.last})

    Phoenix.Channel.push(socket, "phx_reader", reader)
    {:noreply, Socket.assign(socket, :phx_received, count(stream, socket, reader.last))}
  end

  defp take_over(socket) do
    name = {__MODULE__, socket.topic}

    :global.trans({name, self()}, fn ->
      case :global.whereis_name(name) do
        :undefined -> :ok
        replaced -> replace(replaced, socket)
      end

      :yes = :global.re_register_name(name, self())
    end)
  end

  defp replace(replaced, socket) do
    monitor = Process.monitor(replaced)
    emit(:replace, socket)
    send(replaced, :phx_replaced)

    receive do
      {:DOWN, ^monitor, :process, _process, _reason} -> :ok
    after
      @replace_timeout ->
        emit(:kill, socket)
        Process.exit(replaced, :kill)

        receive do
          {:DOWN, ^monitor, :process, _process, _reason} -> :ok
        end
    end
  end

  defp fence, do: Base.url_encode64(:crypto.strong_rand_bytes(16), padding: false)

  defp count(stream, socket, last) do
    received = stream.received(socket)

    case stream.count(socket, last, received) do
      :moved ->
        count(stream, socket, stream.reader(socket).last)

      number ->
        for command <- (last + 1)..(number - 1)//1 do
          emit(:append, socket, %{number: command, received: nil})
        end

        emit(:received, socket, %{received: received})
        emit(:count, socket, %{number: number, received: received})
        received
    end
  end

  defp replaced(socket) do
    emit(:replaced, socket)
    Phoenix.Channel.push(socket, "phx_error", %{})
    {:stop, {:shutdown, :replaced}, socket}
  end

  @doc false
  def unexpected_message(message, socket) do
    process =
      case Process.info(self(), :registered_name) do
        {:registered_name, []} -> self()
        {:registered_name, name} -> name
      end

    :error_logger.warning_msg(
      ~c"~p ~p received unexpected message in handle_info/2: ~p~n",
      [socket.channel, process, message]
    )

    {:noreply, socket}
  end

  @doc false
  def push_durable(_stream, _socket, "phx_reply", _payload),
    do: raise(ArgumentError, "push_durable/3 can't push \"phx_reply\", the event of a reply")

  def push_durable(stream, socket, event, payload),
    do: append(stream, socket, event, payload, socket.assigns[:phx_received])

  defp append(stream, socket, event, payload, received) do
    number = stream.append(socket, event, payload, received)
    emit(:append, socket, %{number: number, received: received})
    push(socket, stream, number, event, payload)
  end

  defp push(%{assigns: %{phx_durable: true}} = socket, stream, number, event, payload) do
    case stream.delivery() do
      :channel ->
        Phoenix.Channel.push(socket, event, {:durable, number, stream.epoch(socket), payload})

      :store ->
        :ok
    end
  end

  defp push(socket, _stream, _number, event, payload),
    do: Phoenix.Channel.push(socket, event, payload)

  defp sync(stream, socket) do
    received = socket.assigns.phx_received
    reader = stream.reader(socket)
    emit(:sync, socket, %{received: received, last: reader.last})
    {:reply, {:ok, %{received: received, reader: reader}}, socket}
  end

  defp read(stream, last, socket) do
    case stream.delivery() do
      :channel -> read_pages(stream, last, socket)
      :store -> {:reply, @invalid, socket}
    end
  end

  defp read_pages(stream, last, socket) do
    page = stream.since(socket, last, @page)

    for {number, event, payload} <- page.messages do
      Phoenix.Channel.push(socket, event, {:durable, number, page.epoch, payload})
    end

    if page.sent < page.latest,
      do: read_pages(stream, page.sent, socket),
      else: {:reply, {:ok, %{sent: page.sent, epoch: page.epoch}}, socket}
  end

  defp handle_durable(stream, number, socket, handle_in) do
    received = socket.assigns.phx_received

    cond do
      number <= received ->
        emit_push(socket, number, :old)
        {:noreply, socket}

      number > received + 1 ->
        emit_push(socket, number, :ahead)
        {:reply, {:error, %{phx_not_taken: "out_of_order"}}, socket}

      true ->
        socket
        |> Socket.assign(:phx_number, number)
        |> run(handle_in)
        |> answered(stream, number)
    end
  end

  defp run(socket, handle_in) do
    handle_in.(socket)
  catch
    kind, reason ->
      Logger.error(Exception.format(kind, reason, __STACKTRACE__))
      {:reply, {:error, %{reason: "crashed"}}, socket}
  end

  defp answered({:retry, reason, socket}, _stream, number) do
    emit_push(socket, number, :retry)
    {:reply, {:error, %{phx_not_taken: reason}}, socket}
  end

  defp answered({:noreply, socket}, stream, number),
    do: {:noreply, answer(stream, number, {:ok, %{}}, socket)}

  defp answered({:noreply, socket, timeout_or_hibernate}, stream, number),
    do: {:noreply, answer(stream, number, {:ok, %{}}, socket), timeout_or_hibernate}

  defp answered({:reply, reply, socket}, stream, number),
    do: {:noreply, answer(stream, number, reply(reply), socket)}

  defp answered({:stop, reason, socket}, stream, number),
    do: answered({:stop, reason, :ok, socket}, stream, number)

  defp answered({:stop, reason, reply, socket}, stream, number),
    do: {:stop, reason, reply(reply), answer(stream, number, reply(reply), socket)}

  defp reply(status) when is_atom(status), do: {status, %{}}
  defp reply({status, response}) when is_atom(status), do: {status, response}

  defp answer(stream, number, {status, response}, socket) do
    emit_push(socket, number, status)

    :ok =
      append(
        stream,
        socket,
        "phx_reply",
        %{number: number, status: status, response: response},
        number
      )

    Socket.assign(socket, :phx_received, number)
  end

  defp emit_push(socket, number, outcome),
    do:
      emit(:push, socket, %{
        number: number,
        received: socket.assigns.phx_received,
        outcome: outcome
      })

  defp emit(event, socket, metadata \\ %{}),
    do:
      :telemetry.execute(
        [:phoenix_durable_channel, event],
        %{},
        Map.merge(%{topic: socket.topic, join_ref: socket.join_ref}, metadata)
      )
end
