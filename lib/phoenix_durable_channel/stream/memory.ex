defmodule PhoenixDurableChannel.Stream.Memory do
  @behaviour PhoenixDurableChannel.Stream
  @behaviour PhoenixDurableChannel.Stream.ThroughChannel
  use GenServer

  def start_link(options), do: GenServer.start_link(__MODULE__, options, name: __MODULE__)

  @impl GenServer
  def init(options) do
    :ets.new(__MODULE__, [:named_table, :public, read_concurrency: true])
    options = Keyword.validate!(options, forget_after: :timer.hours(1), kept: 1000)
    :timer.send_interval(div(options[:forget_after], 4) + 1, :forget)

    {:ok,
     %{
       kept: options[:kept],
       forget_after: options[:forget_after],
       channels: %{},
       counts: %{}
     }}
  end

  @impl GenServer
  def handle_call({:append, topic, event, payload, received}, _from, state) do
    kept = kept(topic)
    number = kept.latest + 1

    messages =
      Enum.take([%{number: number, event: event, payload: payload} | kept.messages], state.kept)

    put(topic, %{kept | latest: number, messages: messages, received: received || kept.received})

    {:reply, number, state}
  end

  def handle_call({:count, topic, last, received}, _from, state) do
    kept = kept(topic)

    if kept.latest == last do
      message = %{number: last + 1, event: "phx_reply", payload: %{received: received}}
      messages = Enum.take([message | kept.messages], state.kept)
      put(topic, %{kept | latest: last + 1, messages: messages, received: received})
      {:reply, last + 1, state}
    else
      {:reply, :moved, state}
    end
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
  def handle_info({:DOWN, _reference, :process, channel, _reason}, state) do
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
    |> Enum.reject(&(:global.whereis_name({PhoenixDurableChannel.Channel, &1}) != :undefined))
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
          received: 0
        }
    end
  end

  @impl PhoenixDurableChannel.Stream
  def received(socket) do
    :ok = GenServer.call(__MODULE__, {:watch, socket.topic, self()})
    kept(socket.topic).received
  end

  @impl PhoenixDurableChannel.Stream
  def count(socket, last, received),
    do: GenServer.call(__MODULE__, {:count, socket.topic, last, received})

  def put(topic, kept),
    do:
      :ets.insert(
        __MODULE__,
        {topic, Map.put(kept, :touched_at, System.monotonic_time(:millisecond))}
      )

  @impl PhoenixDurableChannel.Stream
  def append(socket, event, payload, received),
    do: GenServer.call(__MODULE__, {:append, socket.topic, event, payload, received})

  @impl PhoenixDurableChannel.Stream.ThroughChannel
  def since(socket, number, limit) do
    :ok = GenServer.call(__MODULE__, {:watch, socket.topic, self()})
    kept = kept(socket.topic)
    sent = min(kept.latest, number + limit)

    messages =
      kept.messages
      |> Enum.filter(fn message -> message.number > number and message.number <= sent end)
      |> Enum.reverse()
      |> Enum.map(fn message -> {message.number, message.event, message.payload} end)

    %{messages: messages, sent: sent, latest: kept.latest, epoch: kept.epoch}
  end

  @impl PhoenixDurableChannel.Stream.ThroughChannel
  def epoch(socket), do: kept(socket.topic).epoch

  @impl PhoenixDurableChannel.Stream
  def reader(socket) do
    :ok = GenServer.call(__MODULE__, {:watch, socket.topic, self()})
    kept = kept(socket.topic)
    %{type: "phoenix", last: kept.latest, epoch: kept.epoch}
  end
end
