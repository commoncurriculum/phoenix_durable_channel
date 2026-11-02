defmodule PhoenixDurableChannel.Stream.Memory do
  @behaviour PhoenixDurableChannel.Ledger
  @behaviour PhoenixDurableChannel.Stream
  @behaviour PhoenixDurableChannel.Stream.ThroughChannel
  use GenServer

  @replies_kept 64

  def start_link(options), do: GenServer.start_link(__MODULE__, options, name: __MODULE__)

  @impl GenServer
  def init(options) do
    :ets.new(__MODULE__, [:named_table, :public, read_concurrency: true])
    forget_after = Keyword.get(options, :forget_after, :timer.hours(1))
    :timer.send_interval(div(forget_after, 4) + 1, :forget)

    {:ok,
     %{
       kept: Keyword.get(options, :kept, 1000),
       forget_after: forget_after,
       channels: %{},
       counts: %{}
     }}
  end

  @impl GenServer
  def handle_call({:append, topic, event, payload}, _from, state) do
    kept = kept(topic)
    number = kept.latest + 1

    messages =
      Enum.take([%{number: number, event: event, payload: payload} | kept.messages], state.kept)

    put(topic, %{kept | latest: number, messages: messages})
    {:reply, number, state}
  end

  def handle_call({:put_received, topic, number, reply}, _from, state) do
    kept = kept(topic)

    replies =
      kept.replies |> Map.put(number, reply) |> Map.delete(number - @replies_kept)

    put(topic, %{kept | received: max(kept.received, number), replies: replies})
    {:reply, :ok, state}
  end

  def handle_call({:watch, topic, channel}, _from, state) do
    put(topic, kept(topic))

    if Map.has_key?(state.channels, channel) do
      {:reply, :ok, state}
    else
      Process.monitor(channel)

      {:reply, :ok,
       %{
         state
         | channels: Map.put(state.channels, channel, topic),
           counts: Map.update(state.counts, topic, 1, &(&1 + 1))
       }}
    end
  end

  @impl GenServer
  def handle_info({:DOWN, _ref, :process, channel, _reason}, state) do
    topic = Map.fetch!(state.channels, channel)
    put(topic, kept(topic))
    channels = Map.delete(state.channels, channel)

    case Map.fetch!(state.counts, topic) do
      1 ->
        {:noreply, %{state | channels: channels, counts: Map.delete(state.counts, topic)}}

      count ->
        {:noreply, %{state | channels: channels, counts: Map.put(state.counts, topic, count - 1)}}
    end
  end

  def handle_info(:forget, state) do
    oldest = System.monotonic_time(:millisecond) - state.forget_after

    __MODULE__
    |> :ets.select([{{:"$1", %{touched_at: :"$2"}}, [{:<, :"$2", oldest}], [:"$1"]}])
    |> Enum.reject(&Map.has_key?(state.counts, &1))
    |> Enum.each(&:ets.delete(__MODULE__, &1))

    {:noreply, state}
  end

  def kept(topic) do
    case :ets.lookup(__MODULE__, topic) do
      [{^topic, kept}] ->
        kept

      [] ->
        %{
          epoch: Base.url_encode64(:crypto.strong_rand_bytes(9)),
          latest: 0,
          messages: [],
          received: 0,
          replies: %{}
        }
    end
  end

  @impl PhoenixDurableChannel.Ledger
  def received(socket), do: kept(socket.topic).received

  @impl PhoenixDurableChannel.Ledger
  def put_received(socket, number, reply),
    do: GenServer.call(__MODULE__, {:put_received, socket.topic, number, reply})

  @impl PhoenixDurableChannel.Ledger
  def reply(socket, number), do: Map.get(kept(socket.topic).replies, number)

  def put(topic, kept),
    do:
      :ets.insert(
        __MODULE__,
        {topic, Map.put(kept, :touched_at, System.monotonic_time(:millisecond))}
      )

  @impl PhoenixDurableChannel.Stream
  def append(socket, event, payload),
    do: GenServer.call(__MODULE__, {:append, socket.topic, event, payload})

  @impl PhoenixDurableChannel.Stream.ThroughChannel
  def since(socket, number, limit) do
    kept = kept(socket.topic)
    sent = min(kept.latest, number + limit)

    messages =
      kept.messages
      |> Enum.filter(fn message -> message.number > number and message.number <= sent end)
      |> Enum.reverse()
      |> Enum.map(fn message -> {message.number, message.event, message.payload} end)

    %{messages: messages, sent: sent, latest: kept.latest, epoch: kept.epoch}
  end

  @impl PhoenixDurableChannel.Stream
  def reader(socket) do
    :ok = GenServer.call(__MODULE__, {:watch, socket.topic, self()})
    kept = kept(socket.topic)
    %{type: "phoenix", last: kept.latest, epoch: kept.epoch}
  end
end
