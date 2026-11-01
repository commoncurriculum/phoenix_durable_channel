defmodule PhoenixDurableChannel.Stream.DurableStreams do
  @moduledoc """
  A `PhoenixDurableChannel.Stream` on a [Durable Streams](https://github.com/electric-sql/durable-streams)
  server, such as `durable-streams-cloudflare`: each topic is a JSON stream of the same name
  in one project, which the client reads with the protocol's `live=sse`, so the messages
  never pass through Phoenix.

  Durable Streams offsets are opaque, so each message carries its number: the body is the
  JSON `{number, event, payload}`. The numbers come from the stream itself. A node reads the
  last one the first time it appends to a topic and numbers on from it, sending each
  number as the append's `Stream-Seq`. The server refuses a `Stream-Seq` that is not past
  the stream's last, so when another node took the number first, this node reads on from
  where it last read and appends again: two appends never get the same number.

  The join reply carries a read-scoped JWT for the client's stream, for a day, and the
  offset where its last message ends, so the client's first read starts there.

  It is also a `PhoenixDurableChannel.Ledger`: the reply to the client's durable message n is
  message n of the stream `<topic>/replies`, `{number, event: "reply", payload: {status,
  response}}`, which the client's token cannot read.

      config :phoenix_durable_channel, PhoenixDurableChannel.Stream.DurableStreams,
        url: "https://durable-streams.example.workers.dev",
        reader_url: "https://durable-streams.example.workers.dev", # as browsers reach it
        project: "my-project",
        signing_secret: "..."
  """
  @behaviour PhoenixDurableChannel.Ledger
  @behaviour PhoenixDurableChannel.Stream
  use GenServer

  # One row per topic: {topic, %{given: the last number given out, offset: where the last
  # read ended, last: the last number that read found}}. Two appends that read the same row
  # post the same number, and the store takes one of them.
  @table __MODULE__

  def start_link(options), do: GenServer.start_link(__MODULE__, options, name: __MODULE__)

  @impl GenServer
  def init(_options) do
    :ets.new(@table, [:named_table, :public, write_concurrency: true])
    {:ok, nil}
  end

  @impl PhoenixDurableChannel.Stream
  def append(socket, event, payload) do
    topic = socket.topic
    kept = kept(topic) || read(topic)
    number = kept.given + 1

    # Given out once the store has it: an append that fails leaves no number skipped.
    if post(topic, number, event, payload) == :ok do
      :ets.insert(@table, {topic, %{kept | given: number}})
      number
    else
      read(topic)
      append(socket, event, payload)
    end
  end

  @doc "Appends message `number`: `:ok`, or `:taken` when the stream already has it."
  def post(topic, number, event, payload) do
    response =
      Req.post!(url(topic),
        json: %{number: number, event: event, payload: payload},
        headers: [
          authorization: "Bearer " <> token("write", topic),
          # Compared as strings, so padded to sort as numbers.
          stream_seq: String.pad_leading(Integer.to_string(number), 20, "0")
        ]
      )

    cond do
      response.status in 200..299 -> :ok
      response.status == 409 -> :taken
      true -> raise "Durable Streams refused an append to #{topic}: HTTP #{response.status}"
    end
  end

  @doc "What this node keeps for `topic`, or nil before it first reads it."
  def kept(topic) do
    case :ets.lookup(@table, topic) do
      [{^topic, kept}] -> kept
      [] -> nil
    end
  end

  @doc """
  Reads `topic` on from where this node last read it, creating the stream the first time,
  and numbers on from the last message. Returns what it keeps for the topic.
  """
  def read(topic) do
    start = kept(topic) || create(topic)
    {offset, last} = read_to_end(topic, start.offset, start.last)
    kept = %{given: last, offset: offset, last: last}
    :ets.insert(@table, {topic, kept})
    kept
  end

  @doc "Creates `topic`'s stream; creating one that exists changes nothing."
  def create(topic) do
    Req.put!(url(topic),
      headers: [
        authorization: "Bearer " <> token("write", topic),
        content_type: "application/json"
      ]
    )

    %{given: 0, offset: "-1", last: 0}
  end

  @doc "Reads `topic` from `offset` to the end: where it ended, and the last number."
  def read_to_end(topic, offset, last) do
    response =
      Req.get!(url(topic),
        params: [offset: offset],
        headers: [authorization: "Bearer " <> token("read", topic)],
        decode_body: false
      )

    last =
      response.body
      |> Jason.decode!()
      |> Enum.reduce(last, fn message, _last -> message["number"] end)

    [next_offset] = Req.Response.get_header(response, "stream-next-offset")

    if Req.Response.get_header(response, "stream-up-to-date") == ["true"],
      do: {next_offset, last},
      else: read_to_end(topic, next_offset, last)
  end

  @doc "The stream where `topic`'s ledger keeps its replies."
  def replies(topic), do: topic <> "/replies"

  @impl PhoenixDurableChannel.Ledger
  def received(socket), do: read(replies(socket.topic)).last

  # The store refuses a number it already has, so a repeated record changes nothing.
  @impl PhoenixDurableChannel.Ledger
  def put_received(socket, number, {status, response}) do
    topic = replies(socket.topic)
    kept(topic) || read(topic)
    post(topic, number, "reply", %{status: status, response: response})
    :ok
  end

  @impl PhoenixDurableChannel.Ledger
  def reply(socket, number), do: find(replies(socket.topic), "-1", number)

  @doc "The reply numbered `number` in `topic`, reading from `offset`."
  def find(topic, offset, number) do
    response =
      Req.get!(url(topic),
        params: [offset: offset],
        headers: [authorization: "Bearer " <> token("read", topic)],
        decode_body: false
      )

    found = response.body |> Jason.decode!() |> Enum.find(&(&1["number"] == number))
    [next_offset] = Req.Response.get_header(response, "stream-next-offset")

    case found do
      %{"payload" => %{"status" => status, "response" => response}} ->
        {String.to_existing_atom(status), response}

      nil ->
        find(topic, next_offset, number)
    end
  end

  @impl PhoenixDurableChannel.Stream
  def reader(socket) do
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

  @doc """
  A JWT for `topic` with `scope` (\"read\" or \"write\"), signed with the project's secret,
  for `seconds` (a day). A client whose read token expired asks for a fresh reader.
  """
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
