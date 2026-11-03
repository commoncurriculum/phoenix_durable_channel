defmodule PhoenixDurableChannel.Stream.DurableStreams do
  @behaviour PhoenixDurableChannel.Stream
  use GenServer

  @table __MODULE__

  def start_link(options), do: GenServer.start_link(__MODULE__, options, name: __MODULE__)

  @impl GenServer
  def init(options) do
    :ets.new(@table, [:named_table, :public, write_concurrency: true])

    {:ok,
     %{
       channels: %{},
       topics: %{},
       keep_offsets_for:
         options
         |> Keyword.validate!(keep_offsets_for: :timer.minutes(5))
         |> Keyword.fetch!(:keep_offsets_for)
     }}
  end

  @impl GenServer
  def handle_cast({:watch, topic, channel}, state) do
    if Map.has_key?(state.channels, channel) do
      {:noreply, state}
    else
      Process.monitor(channel)

      {:noreply,
       %{
         state
         | channels: Map.put(state.channels, channel, topic),
           topics:
             Map.update(state.topics, topic, 1, fn
               count when is_integer(count) -> count + 1
               _forget -> 1
             end)
       }}
    end
  end

  @impl GenServer
  def handle_info({:DOWN, _reference, :process, channel, _reason}, state) do
    topic = Map.fetch!(state.channels, channel)
    count = Map.fetch!(state.topics, topic) - 1
    state = %{state | channels: Map.delete(state.channels, channel)}

    if count == 0 do
      reference = make_ref()
      Process.send_after(self(), {:forget, topic, reference}, state.keep_offsets_for)
      {:noreply, %{state | topics: Map.put(state.topics, topic, reference)}}
    else
      {:noreply, %{state | topics: Map.put(state.topics, topic, count)}}
    end
  end

  def handle_info({:forget, topic, reference}, state) do
    if Map.get(state.topics, topic) == reference do
      :ets.delete(@table, topic)
      {:noreply, %{state | topics: Map.delete(state.topics, topic)}}
    else
      {:noreply, state}
    end
  end

  @impl PhoenixDurableChannel.Stream
  def append(socket, event, payload, received) do
    topic = socket.topic
    kept = kept(topic) || read(topic)
    number = kept.given + 1

    case post(topic, number, event, payload, received) do
      :taken ->
        read(topic)
        append(socket, event, payload, received)

      :posted ->
        :ets.insert(@table, {topic, %{kept | given: number}})
        number
    end
  end

  @impl PhoenixDurableChannel.Stream
  def count(socket, last, received) do
    topic = socket.topic

    case post(topic, last + 1, "phx_reply", %{received: received}, received) do
      :taken ->
        read(topic)
        :moved

      :posted ->
        kept = kept(topic)
        :ets.insert(@table, {topic, %{kept | given: max(kept.given, last + 1)}})
        last + 1
    end
  end

  def post(topic, number, event, payload, received) do
    response =
      Req.post!(url(topic),
        json: %{number: number, event: event, payload: payload, received: received},
        headers: [
          authorization: "Bearer " <> token("write", topic),
          stream_seq: String.pad_leading(Integer.to_string(number), 20, "0")
        ]
      )

    cond do
      response.status in 200..299 -> :posted
      response.status == 409 -> :taken
      true -> raise "Durable Streams refused an append to #{topic}: HTTP #{response.status}"
    end
  end

  def kept(topic) do
    case :ets.lookup(@table, topic) do
      [{^topic, kept}] -> kept
      [] -> nil
    end
  end

  def read(topic) do
    known =
      case kept(topic) do
        nil ->
          Req.put!(url(topic),
            headers: [
              authorization: "Bearer " <> token("write", topic),
              content_type: "application/json"
            ]
          )

          %{given: 0, offset: "-1", last: 0, received: 0}

        kept ->
          kept
      end

    kept = read_to_end(topic, known)
    :ets.insert(@table, {topic, kept})
    kept
  end

  def read_to_end(topic, kept) do
    response = get(topic, kept.offset)

    read =
      response.body
      |> Jason.decode!()
      |> Enum.reduce(kept, fn message, kept ->
        if message["number"] > kept.last,
          do: %{kept | last: message["number"], received: message["received"] || kept.received},
          else: kept
      end)

    read = %{
      read
      | offset: hd(Req.Response.get_header(response, "stream-next-offset")),
        given: read.last
    }

    if Req.Response.get_header(response, "stream-up-to-date") == ["true"],
      do: read,
      else: read_to_end(topic, read)
  end

  def get(topic, offset),
    do:
      Req.get!(url(topic),
        params: [offset: offset],
        headers: [authorization: "Bearer " <> token("read", topic)],
        decode_body: false
      )

  @impl PhoenixDurableChannel.Stream
  def received(socket) do
    GenServer.cast(__MODULE__, {:watch, socket.topic, self()})
    read(socket.topic).received
  end

  @impl PhoenixDurableChannel.Stream
  def reader(socket) do
    GenServer.cast(__MODULE__, {:watch, socket.topic, self()})
    config = config()
    path = "/v1/stream/#{config[:project]}/#{URI.encode_www_form(socket.topic)}"
    kept = read(socket.topic)

    %{
      type: "durable-streams",
      url: Keyword.get(config, :reader_url, config[:url]) <> path,
      token: token("read", socket.topic),
      last: kept.last,
      offset: kept.offset
    }
  end

  def token(scope, topic, seconds \\ 86_400) do
    config = config()
    header = Base.url_encode64(Jason.encode!(%{alg: "HS256", typ: "JWT"}), padding: false)

    claims =
      %{
        sub: config[:project],
        scope: scope,
        stream_id: topic,
        exp: System.os_time(:second) + seconds
      }
      |> Jason.encode!()
      |> Base.url_encode64(padding: false)

    signature =
      :hmac
      |> :crypto.mac(:sha256, config[:signing_secret], header <> "." <> claims)
      |> Base.url_encode64(padding: false)

    header <> "." <> claims <> "." <> signature
  end

  def url(topic),
    do: config()[:url] <> "/v1/stream/#{config()[:project]}/#{URI.encode_www_form(topic)}"

  def config, do: Application.fetch_env!(:phoenix_durable_channel, __MODULE__)
end
