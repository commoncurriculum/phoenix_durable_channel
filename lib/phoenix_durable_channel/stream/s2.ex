defmodule PhoenixDurableChannel.Stream.S2 do
  @behaviour PhoenixDurableChannel.Stream

  @impl PhoenixDurableChannel.Stream
  def append(socket, event, payload, received) do
    record = %S2.V1.AppendRecord{
      body: Jason.encode!(%{event: event, payload: payload, received: received})
    }

    case S2.append(config()[:name], socket.topic, [record]) do
      {:ok, %S2.V1.AppendAck{end: %S2.V1.StreamPosition{seq_num: number}}} ->
        number

      {:error, error} ->
        raise "S2 refused an append to #{socket.topic}: #{inspect(error)}"
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
  def received(socket) do
    case S2.read(config()[:name], socket.topic, tail_offset: 1, count: 1, clamp: true) do
      {:ok, %S2.V1.ReadBatch{records: [record]}} ->
        case Jason.decode(record.body) do
          {:ok, %{"received" => received}} -> received
          _command -> 0
        end

      {:ok, %S2.V1.ReadBatch{records: []}} ->
        0

      {:error, %S2.Error{status: 416}} ->
        0

      {:error, error} ->
        raise "S2 refused a read of #{socket.topic}: #{inspect(error)}"
    end
  end

  def config, do: Application.fetch_env!(:phoenix_durable_channel, __MODULE__)
end
