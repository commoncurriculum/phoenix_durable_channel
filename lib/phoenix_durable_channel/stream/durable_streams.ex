defmodule PhoenixDurableChannel.Stream.DurableStreams do
  @behaviour PhoenixDurableChannel.Ledger
  @behaviour PhoenixDurableChannel.Stream
  use GenServer

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

    if post(topic, number, event, payload) == :ok do
      :ets.insert(@table, {topic, %{kept | given: number}})
      number
    else
      read(topic)
      append(socket, event, payload)
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
      response.status in 200..299 -> :ok
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
    start = kept(topic) || create(topic)
    {offset, last} = read_to_end(topic, start.offset, start.last)
    kept = %{given: last, offset: offset, last: last}
    :ets.insert(@table, {topic, kept})
    kept
  end

  def create(topic) do
    Req.put!(url(topic),
      headers: [
        authorization: "Bearer " <> token("write", topic),
        content_type: "application/json"
      ]
    )

    %{given: 0, offset: "-1", last: 0}
  end

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

  def replies(topic), do: topic <> "/replies"

  @impl PhoenixDurableChannel.Ledger
  def received(socket), do: read(replies(socket.topic)).last

  @impl PhoenixDurableChannel.Ledger
  def put_received(socket, number, {status, response}) do
    topic = replies(socket.topic)
    kept(topic) || read(topic)
    post(topic, number, "reply", %{status: status, response: response})
    :ok
  end

  @impl PhoenixDurableChannel.Ledger
  def reply(socket, number), do: find(replies(socket.topic), "-1", number)

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
