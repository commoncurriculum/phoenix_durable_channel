defmodule PhoenixDurableChannel.Stream.S2 do
  @behaviour PhoenixDurableChannel.Ledger
  @behaviour PhoenixDurableChannel.Stream

  @impl PhoenixDurableChannel.Stream
  def append(socket, event, payload) do
    record = %S2.V1.AppendRecord{body: Jason.encode!(%{event: event, payload: payload})}

    case S2.append(config()[:name], socket.topic, [record]) do
      {:ok, %S2.V1.AppendAck{end: %S2.V1.StreamPosition{seq_num: number}}} -> number
    end
  end

  @impl PhoenixDurableChannel.Stream
  def reader(socket) do
    %{
      type: "s2",
      endpoint: config()[:basin_endpoint],
      basin: config()[:basin],
      stream: socket.topic,
      token: read_token(socket.topic),
      last: last(socket.topic)
    }
  end

  def last(stream) do
    case S2.check_tail(config()[:name], stream) do
      {:ok, %S2.V1.StreamPosition{seq_num: last}} -> last
    end
  end

  def read_token(stream) do
    if config()[:account_endpoint] do
      client =
        S2.Client.new(
          S2.Config.new(base_url: config()[:account_endpoint], token: config()[:token])
        )

      info = %S2.AccessTokenInfo{
        id: "#{stream}/#{System.unique_integer([:positive])}-#{System.system_time(:millisecond)}",
        expires_at: DateTime.add(DateTime.utc_now(), 1, :day),
        scope: %S2.AccessTokenScope{
          basins: %{exact: config()[:basin]},
          streams: %{exact: stream},
          ops: ["read"]
        }
      }

      case S2.AccessTokens.issue_access_token(info, server: client) do
        {:ok, %S2.IssueAccessTokenResponse{access_token: token}} -> token
      end
    end
  end

  def replies(topic), do: topic <> "/replies"

  @impl PhoenixDurableChannel.Ledger
  def received(socket) do
    case S2.read(config()[:name], replies(socket.topic), tail_offset: 1, count: 1, clamp: true) do
      {:ok, %S2.V1.ReadBatch{records: [record]}} -> Jason.decode!(record.body)["number"]
      {:error, %S2.Error{status: 416}} -> 0
    end
  end

  @impl PhoenixDurableChannel.Ledger
  def put_received(socket, number, {status, response}) do
    body = Jason.encode!(%{number: number, status: status, response: response})

    case S2.append(config()[:name], replies(socket.topic), [%S2.V1.AppendRecord{body: body}]) do
      {:ok, _ack} -> :ok
    end
  end

  @impl PhoenixDurableChannel.Ledger
  def reply(socket, number), do: reply(socket, number, number - 1)

  def reply(socket, number, from) do
    case S2.read(config()[:name], replies(socket.topic), seq_num: from, count: 64) do
      {:ok, %S2.V1.ReadBatch{records: []}} ->
        nil

      {:ok, %S2.V1.ReadBatch{records: records}} ->
        replies = Enum.map(records, fn record -> Jason.decode!(record.body) end)

        case Enum.find(replies, fn reply -> reply["number"] == number end) do
          %{"status" => status, "response" => response} ->
            {String.to_existing_atom(status), response}

          nil ->
            reply(socket, number, from + length(records))
        end

      {:error, %S2.Error{status: 416}} ->
        nil
    end
  end

  def config, do: Application.fetch_env!(:phoenix_durable_channel, __MODULE__)
end
