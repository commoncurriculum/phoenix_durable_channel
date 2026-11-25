defmodule PhoenixDurableChannel.Channel do
  require Logger

  alias Phoenix.Socket

  @replace_timeout 5_000
  @page 500

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
    defines_handle_info = Module.defines?(env.module, {:handle_info, 2})
    defines_handle_in = Module.defines?(env.module, {:handle_in, 3})

    other_message =
      if defines_handle_info,
        do: quote(do: super(message, socket)),
        else: quote(do: PhoenixDurableChannel.Channel.unexpected_message(message, socket))

    handle_in =
      if defines_handle_in do
        quote do
          def handle_in(event, {:durable, number, payload}, socket) do
            PhoenixDurableChannel.Channel.handle_durable(
              durable_stream(socket),
              number,
              socket,
              fn socket -> super(event, payload, socket) end
            )
          end

          def handle_in(event, payload, socket), do: super(event, payload, socket)
        end
      end

    quote do
      defoverridable join: 3

      if unquote(defines_handle_in), do: defoverridable(handle_in: 3)

      def join(topic, params, socket),
        do: PhoenixDurableChannel.Channel.join(params, fn -> super(topic, params, socket) end)

      def handle_in("phx_sync", _payload, %{assigns: %{phx_durable: true}} = socket),
        do: PhoenixDurableChannel.Channel.sync(durable_stream(socket), socket)

      def handle_in("phx_read", %{"after" => last}, %{assigns: %{phx_durable: true}} = socket)
          when is_integer(last) and last >= 0,
          do: PhoenixDurableChannel.Channel.read(durable_stream(socket), last, socket)

      def handle_in("phx_reader", _payload, %{assigns: %{phx_durable: true}} = socket),
        do: {:reply, {:ok, durable_stream(socket).reader(socket)}, socket}

      def handle_in(event, _payload, socket) when event in ["phx_sync", "phx_read", "phx_reader"],
        do: {:reply, {:error, %{reason: "invalid"}}, socket}

      def handle_in(_event, {:durable, _number, _payload}, %{assigns: assigns} = socket)
          when not is_map_key(assigns, :phx_durable),
          do: {:reply, {:error, %{reason: "invalid"}}, socket}

      def handle_in(_event, {:durable, number, _payload}, socket)
          when not is_integer(number) or number < 1,
          do: {:reply, {:error, %{reason: "invalid"}}, socket}

      unquote(handle_in)

      if unquote(defines_handle_info), do: defoverridable(handle_info: 2)

      def handle_info(:phx_replaced, socket),
        do: PhoenixDurableChannel.Channel.replaced(socket)

      def handle_info(:phx_durable_joined, socket),
        do: PhoenixDurableChannel.Channel.joined(&durable_stream/1, socket)

      def handle_info(message, socket), do: unquote(other_message)
    end
  end

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

  @doc false
  def joined(stream_for, socket) do
    take_over(socket)
    emit(:register, socket)

    socket = Socket.assign(socket, :phx_fence, fence())
    stream = stream_for.(socket)
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
  def replaced(socket) do
    emit(:replaced, socket)
    Phoenix.Channel.push(socket, "phx_error", %{})
    {:stop, {:shutdown, :replaced}, socket}
  end

  @doc false
  def push_durable(_stream, _socket, "phx_reply", _payload),
    do: raise(ArgumentError, "push_durable/3 can't push \"phx_reply\", the event of a reply")

  def push_durable(stream, socket, event, payload),
    do: append(stream, socket, event, payload, socket.assigns[:phx_received])

  defp append(stream, socket, event, payload, received) do
    number = stream.append(socket, event, payload, received)
    emit(:append, socket, %{number: number, received: received})

    case socket.assigns do
      %{phx_durable: true} ->
        if through_channel?(stream),
          do:
            Phoenix.Channel.push(socket, event, {:durable, number, stream.epoch(socket), payload})

      _plain ->
        Phoenix.Channel.push(socket, event, payload)
    end

    :ok
  end

  defp through_channel?(stream),
    do: Code.ensure_loaded?(stream) and function_exported?(stream, :since, 3)

  @doc false
  def sync(stream, socket) do
    received = socket.assigns.phx_received
    reader = stream.reader(socket)
    emit(:sync, socket, %{received: received, last: reader.last})
    {:reply, {:ok, %{received: received, reader: reader}}, socket}
  end

  @doc false
  def read(stream, last, socket) do
    if through_channel?(stream),
      do: read_pages(stream, last, socket),
      else: {:reply, {:error, %{reason: "invalid"}}, socket}
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

  @doc false
  def handle_durable(stream, number, socket, handle_in) do
    received = socket.assigns.phx_received

    cond do
      number <= received ->
        emit_push(socket, number, received, :old)
        {:noreply, socket}

      number > received + 1 ->
        emit_push(socket, number, received, :ahead)
        {:reply, {:error, %{phx_not_taken: "out_of_order"}}, socket}

      true ->
        socket
        |> Socket.assign(:phx_number, number)
        |> run(handle_in)
        |> answered(stream, number, received)
    end
  end

  defp run(socket, handle_in) do
    handle_in.(socket)
  catch
    kind, reason ->
      Logger.error(Exception.format(kind, reason, __STACKTRACE__))
      {:reply, {:error, %{reason: "crashed"}}, socket}
  end

  defp answered({:retry, reason, socket}, _stream, number, received) do
    emit_push(socket, number, received, :retry)
    {:reply, {:error, %{phx_not_taken: reason}}, socket}
  end

  defp answered({:noreply, socket}, stream, number, _received),
    do: {:noreply, answer(stream, number, {:ok, %{}}, socket)}

  defp answered({:noreply, socket, timeout_or_hibernate}, stream, number, _received),
    do: {:noreply, answer(stream, number, {:ok, %{}}, socket), timeout_or_hibernate}

  defp answered({:reply, status, socket}, stream, number, _received) when is_atom(status),
    do: {:noreply, answer(stream, number, {status, %{}}, socket)}

  defp answered({:reply, reply, socket}, stream, number, _received),
    do: {:noreply, answer(stream, number, reply, socket)}

  defp answered({:stop, reason, socket}, stream, number, _received),
    do: {:stop, reason, {:ok, %{}}, answer(stream, number, {:ok, %{}}, socket)}

  defp answered({:stop, reason, status, socket}, stream, number, _received) when is_atom(status),
    do: {:stop, reason, {status, %{}}, answer(stream, number, {status, %{}}, socket)}

  defp answered({:stop, reason, reply, socket}, stream, number, _received),
    do: {:stop, reason, reply, answer(stream, number, reply, socket)}

  defp answer(stream, number, {status, response}, socket) do
    emit_push(socket, number, socket.assigns.phx_received, status)

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

  defp emit_push(socket, number, received, outcome),
    do: emit(:push, socket, %{number: number, received: received, outcome: outcome})

  defp emit(event, socket, metadata \\ %{}),
    do:
      :telemetry.execute(
        [:phoenix_durable_channel, event],
        %{},
        Map.merge(%{topic: socket.topic, join_ref: socket.join_ref}, metadata)
      )
end
