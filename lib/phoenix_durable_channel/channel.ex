defmodule PhoenixDurableChannel.Channel do
  require Logger

  @replace_timeout 5_000
  @page 500
  @replies_kept 32

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

          def handle_info({:phx_durable_joined, ledger_for}, socket),
            do: PhoenixDurableChannel.Channel.joined(ledger_for, socket)

          def handle_info(message, socket), do: super(message, socket)
        end
      else
        quote do
          def handle_info(:phx_replaced, socket),
            do: PhoenixDurableChannel.Channel.replaced(socket)

          def handle_info({:phx_durable_joined, ledger_for}, socket),
            do: PhoenixDurableChannel.Channel.joined(ledger_for, socket)

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
            &durable_ledger/1,
            &durable_stream/1
          )

      def handle_in("phx_sync", _payload, socket),
        do: PhoenixDurableChannel.Channel.sync(socket)

      def handle_in("phx_read", %{"after" => last}, socket),
        do: PhoenixDurableChannel.Channel.read(durable_stream(socket), last, socket)

      def handle_in("phx_reader", _payload, socket),
        do: {:reply, {:ok, durable_stream(socket).reader(socket)}, socket}

      unquote(handle_in)

      unquote(handle_info)
    end
  end

  def join({:ok, socket}, %{"phx_durable" => true} = params, ledger_for, stream_for),
    do: join({:ok, %{}, socket}, params, ledger_for, stream_for)

  def join({:ok, reply, socket}, %{"phx_durable" => true}, ledger_for, stream_for)
      when is_map(reply) do
    send(self(), {:phx_durable_joined, ledger_for})
    socket = Phoenix.Socket.assign(socket, phx_durable: true, phx_replies: %{})
    {:ok, Map.put(reply, :phx_reader, stream_for.(socket).reader(socket)), socket}
  end

  def join(result, _params, _ledger_for, _stream_for), do: result

  def joined(ledger_for, socket) do
    name = {__MODULE__, socket.topic}

    :global.trans({name, self()}, fn ->
      replaced = :global.whereis_name(name)

      if replaced != :undefined do
        monitor = Process.monitor(replaced)
        send(replaced, :phx_replaced)

        receive do
          {:DOWN, ^monitor, :process, _process, _reason} -> :ok
        after
          @replace_timeout ->
            Process.exit(replaced, :kill)

            receive do
              {:DOWN, ^monitor, :process, _process, _reason} -> :ok
            end
        end
      end

      :yes = :global.re_register_name(name, self())
    end)

    {:noreply, Phoenix.Socket.assign(socket, :phx_received, ledger_for.(socket).received(socket))}
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

  def push_durable(stream, socket, event, payload) do
    number = stream.append(socket, event, payload)

    if function_exported?(stream, :since, 3),
      do: Phoenix.Channel.push(socket, event, {:durable, number, payload})

    :ok
  end

  def sync(socket), do: {:reply, {:ok, %{received: socket.assigns.phx_received}}, socket}

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
    received = socket.assigns.phx_received

    cond do
      number <= received ->
        kept =
          Map.get_lazy(socket.assigns.phx_replies, number, fn ->
            ledger.reply(socket, number) || {:error, %{reason: "reply_lost"}}
          end)

        {:reply, kept, socket}

      number > received + 1 ->
        {:reply, {:error, %{phx_not_taken: "out_of_order"}}, socket}

      true ->
        result =
          try do
            handle_in.(Phoenix.Socket.assign(socket, :phx_number, number))
          catch
            kind, reason ->
              Logger.error(Exception.format(kind, reason, __STACKTRACE__))
              {:reply, {:error, %{reason: "crashed"}}, socket}
          end

        case result do
          {:retry, reason, socket} ->
            {:reply, {:error, %{phx_not_taken: reason}}, socket}

          {:noreply, socket} ->
            record(ledger, number, {:ok, %{}}, socket, &{:reply, &1, &2})

          {:noreply, socket, _timeout_or_hibernate} ->
            record(ledger, number, {:ok, %{}}, socket, &{:reply, &1, &2})

          {:reply, status, socket} when is_atom(status) ->
            record(ledger, number, {status, %{}}, socket, &{:reply, &1, &2})

          {:reply, reply, socket} ->
            record(ledger, number, reply, socket, &{:reply, &1, &2})

          {:stop, reason, socket} ->
            record(ledger, number, {:ok, %{}}, socket, &{:stop, reason, &1, &2})

          {:stop, reason, status, socket} when is_atom(status) ->
            record(ledger, number, {status, %{}}, socket, &{:stop, reason, &1, &2})

          {:stop, reason, reply, socket} ->
            record(ledger, number, reply, socket, &{:stop, reason, &1, &2})
        end
    end
  end

  def record(ledger, number, reply, socket, answer) do
    :ok = ledger.put_received(socket, number, reply)

    replies =
      socket.assigns.phx_replies
      |> Map.put(number, reply)
      |> Map.delete(number - @replies_kept)

    answer.(reply, Phoenix.Socket.assign(socket, phx_received: number, phx_replies: replies))
  end
end
