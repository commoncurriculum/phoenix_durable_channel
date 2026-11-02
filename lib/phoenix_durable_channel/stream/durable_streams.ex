defmodule PhoenixDurableChannel.Stream.DurableStreams do
  @behaviour PhoenixDurableChannel.Ledger
  @behaviour PhoenixDurableChannel.Stream
  use GenServer

  @table __MODULE__
  @replies_kept 64

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
      :ets.delete(@table, replies(topic))
      {:noreply, %{state | topics: Map.delete(state.topics, topic)}}
    else
      {:noreply, state}
    end
  end

  @impl PhoenixDurableChannel.Stream
  def append(socket, event, payload) do
    topic = socket.topic
    kept = kept(topic) || read(topic)
    number = kept.given + 1

    case post(topic, number, event, payload) do
      :taken ->
        read(topic)
        append(socket, event, payload)

      _offset ->
        :ets.insert(@table, {topic, %{kept | given: number}})
        number
    end
  end

  def post(topic, number, event, payload) do
    response =
      Req.post!(url(topic),
        json: %{number: number, event: event, payload: payload},
        headers: [
          authorization: "Bearer " <> token("write", topic),
          stream_seq: String.pad_leading(Integer.to_string(number), 20, "0")
        ]
      )

    cond do
      response.status in 200..299 -> hd(Req.Response.get_header(response, "stream-next-offset"))
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

          %{given: 0, offset: "-1", last: 0, offsets: %{}}

        kept ->
          kept
      end

    kept = read_to_end(topic, known)
    :ets.insert(@table, {topic, kept})
    kept
  end

  def read_to_end(topic, kept) do
    response = get(topic, kept.offset)

    last =
      response.body
      |> Jason.decode!()
      |> Enum.reduce(kept.last, fn message, last -> max(last, message["number"]) end)

    offset = hd(Req.Response.get_header(response, "stream-next-offset"))

    read = %{
      kept
      | offset: offset,
        last: last,
        given: last,
        offsets:
          kept.offsets
          |> Map.put(last, offset)
          |> Enum.sort()
          |> Enum.take(-@replies_kept)
          |> Map.new()
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

  def replies(topic), do: topic <> "/replies"

  @impl PhoenixDurableChannel.Ledger
  def received(socket) do
    GenServer.cast(__MODULE__, {:watch, socket.topic, self()})
    read(replies(socket.topic)).last
  end

  @impl PhoenixDurableChannel.Ledger
  def put_received(socket, number, {status, response}) do
    topic = replies(socket.topic)
    offset = post(topic, number, "reply", %{status: status, response: response})
    kept = kept(topic) || read(topic)

    offsets =
      if offset == :taken,
        do: kept.offsets,
        else:
          kept.offsets
          |> Map.put(number, offset)
          |> Enum.sort()
          |> Enum.take(-@replies_kept)
          |> Map.new()

    :ets.insert(@table, {topic, %{kept | last: max(kept.last, number), offsets: offsets}})
    :ok
  end

  @impl PhoenixDurableChannel.Ledger
  def reply(socket, number) do
    topic = replies(socket.topic)

    offset =
      (kept(topic) || read(topic)).offsets
      |> Enum.filter(fn {before, _offset} -> before < number end)
      |> Enum.max_by(fn {before, _offset} -> before end, fn -> {0, "-1"} end)
      |> elem(1)

    find(topic, offset, number)
  end

  def find(topic, offset, number) do
    response = get(topic, offset)
    found = response.body |> Jason.decode!() |> Enum.find(&(&1["number"] == number))

    cond do
      found != nil ->
        {String.to_existing_atom(found["payload"]["status"]), found["payload"]["response"]}

      Req.Response.get_header(response, "stream-up-to-date") == ["true"] ->
        nil

      true ->
        find(topic, hd(Req.Response.get_header(response, "stream-next-offset")), number)
    end
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
