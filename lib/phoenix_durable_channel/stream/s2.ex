defmodule PhoenixDurableChannel.Stream.S2 do
  @behaviour PhoenixDurableChannel.Stream

  @first_page 16
  @largest_page 1000

  @impl PhoenixDurableChannel.Stream
  def append(socket, event, payload, received) do
    record = %S2.V1.AppendRecord{
      body: Jason.encode!(%{event: event, payload: payload, received: received})
    }

    fence =
      if Map.has_key?(socket.assigns, :phx_fence),
        do: [fencing_token: socket.assigns.phx_fence],
        else: []

    case S2.append(config()[:name], socket.topic, [record], fence) do
      {:ok, %S2.V1.AppendAck{end: %S2.V1.StreamPosition{seq_num: number}}} ->
        number

      {:error, error} ->
        raise "S2 refused an append to #{socket.topic}: #{inspect(error)}"
    end
  end

  @impl PhoenixDurableChannel.Stream
  def count(socket, last, received) do
    records = [
      %S2.V1.AppendRecord{
        headers: [%S2.V1.Header{name: "", value: "fence"}],
        body: socket.assigns.phx_fence
      },
      %S2.V1.AppendRecord{
        body: Jason.encode!(%{event: "phx_reply", payload: %{received: received}, received: received})
      }
    ]

    case S2.append(config()[:name], socket.topic, records, match_seq_num: last) do
      {:ok, %S2.V1.AppendAck{end: %S2.V1.StreamPosition{seq_num: number}}} -> number
      {:error, %S2.Error{code: "seq_num_mismatch"}} -> :moved
      {:error, error} -> raise "S2 refused the count of #{socket.topic}: #{inspect(error)}"
    end
  end

  @impl PhoenixDurableChannel.Stream
  def reader(socket) do
    token =
      if config()[:account_endpoint] do
        client =
          S2.Client.new(
            S2.Config.new(base_url: config()[:account_endpoint], token: config()[:token])
          )

        info = %S2.AccessTokenInfo{
          id:
            "#{socket.topic}/#{System.unique_integer([:positive])}-#{System.system_time(:millisecond)}",
          expires_at: DateTime.add(DateTime.utc_now(), 1, :day),
          scope: %S2.AccessTokenScope{
            basins: %{exact: config()[:basin]},
            streams: %{exact: socket.topic},
            ops: ["read"]
          }
        }

        case S2.AccessTokens.issue_access_token(info, server: client) do
          {:ok, %S2.IssueAccessTokenResponse{access_token: token}} ->
            token

          {:error, error} ->
            raise "S2 refused a read token for #{socket.topic}: #{inspect(error)}"
        end
      end

    %{
      type: "s2",
      endpoint: config()[:basin_endpoint],
      basin: config()[:basin],
      stream: socket.topic,
      token: token,
      last: last(socket.topic)
    }
  end

  def last(stream) do
    case S2.check_tail(config()[:name], stream) do
      {:ok, %S2.V1.StreamPosition{seq_num: last}} -> last
      {:error, error} -> raise "S2 refused the tail of #{stream}: #{inspect(error)}"
    end
  end

  @impl PhoenixDurableChannel.Stream
  def received(socket), do: received(socket.topic, last(socket.topic), @first_page)

  def received(_stream, 0, _page), do: 0

  def received(stream, before, page) do
    start = max(before - page, 0)

    counts =
      stream
      |> records(start, before)
      |> Enum.flat_map(fn record ->
        case Jason.decode(record.body) do
          {:ok, %{"received" => received}} when is_integer(received) -> [received]
          _no_count -> []
        end
      end)

    if counts == [],
      do: received(stream, start, min(page * 2, @largest_page)),
      else: List.last(counts)
  end

  def records(stream, from, before) do
    case S2.read(config()[:name], stream, seq_num: from, count: before - from, clamp: true) do
      {:ok, %S2.V1.ReadBatch{records: []}} ->
        []

      {:ok, %S2.V1.ReadBatch{records: records}} ->
        next = List.last(records).seq_num + 1
        read = Enum.filter(records, fn record -> record.seq_num < before end)
        if next < before, do: read ++ records(stream, next, before), else: read

      {:error, error} ->
        raise "S2 refused a read of #{stream}: #{inspect(error)}"
    end
  end

  def config, do: Application.fetch_env!(:phoenix_durable_channel, __MODULE__)
end
