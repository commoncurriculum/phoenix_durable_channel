defmodule PhoenixDurableChannel.Stream.S2 do
  @moduledoc """
  A `PhoenixDurableChannel.Stream` on [S2](https://s2.dev): each topic is an S2 stream of the same
  name, which the client reads with S2's own server-sent events, so the messages never pass
  through Phoenix.

  Each record's body is the JSON `{event, payload}`, and record `seq_num` n is message number
  n + 1. The basin must create streams on append and on read. The join reply carries a token
  that can only read the client's own stream, for a day; without an account endpoint, as on
  S2 Lite, the client reads without one.

  It is also a `PhoenixDurableChannel.Ledger`: the replies to the client's durable messages go to
  the stream `<topic>/replies`, each the JSON `{number, status, response}`, which the
  client's token cannot read.

      config :phoenix_durable_channel, PhoenixDurableChannel.Stream.S2,
        name: MyApp.S2,                         # an `S2` client started with this basin
        basin: "my-basin",
        basin_endpoint: "https://{basin}.b.aws.s2.dev",
        account_endpoint: "https://aws.s2.dev", # nil on S2 Lite
        token: "..."                            # issues the read tokens
  """
  @behaviour PhoenixDurableChannel.Ledger
  @behaviour PhoenixDurableChannel.Stream

  @impl PhoenixDurableChannel.Stream
  def append(socket, event, payload) do
    record = %S2.V1.AppendRecord{body: Jason.encode!(%{event: event, payload: payload})}

    {:ok, %S2.V1.AppendAck{end: %S2.V1.StreamPosition{seq_num: number}}} =
      S2.append(config()[:name], socket.topic, [record])

    number
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

  @doc "The number of `stream`'s last message: the `seq_num` of its tail."
  def last(stream) do
    {:ok, %S2.V1.StreamPosition{seq_num: last}} = S2.check_tail(config()[:name], stream)
    last
  end

  @doc "A token that can only read `stream`, for a day. Nil without an account endpoint."
  def read_token(stream) do
    if config()[:account_endpoint] do
      client =
        S2.Client.new(
          S2.Config.new(base_url: config()[:account_endpoint], token: config()[:token])
        )

      {:ok, %S2.IssueAccessTokenResponse{access_token: token}} =
        S2.AccessTokens.issue_access_token(
          %S2.AccessTokenInfo{
            id:
              "#{stream}/#{System.unique_integer([:positive])}-#{System.system_time(:millisecond)}",
            expires_at: DateTime.add(DateTime.utc_now(), 1, :day),
            scope: %S2.AccessTokenScope{
              basins: %{exact: config()[:basin]},
              streams: %{exact: stream},
              ops: ["read"]
            }
          },
          server: client
        )

      token
    end
  end

  @doc "The S2 stream where `topic`'s ledger keeps its replies."
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

    {:ok, _ack} =
      S2.append(config()[:name], replies(socket.topic), [%S2.V1.AppendRecord{body: body}])

    :ok
  end

  # Reply n is record n - 1, or later when an append was repeated.
  @impl PhoenixDurableChannel.Ledger
  def reply(socket, number), do: reply(socket, number, number - 1)

  def reply(socket, number, from) do
    {:ok, %S2.V1.ReadBatch{records: records}} =
      S2.read(config()[:name], replies(socket.topic), seq_num: from, count: 64)

    replies = Enum.map(records, fn record -> Jason.decode!(record.body) end)

    case Enum.find(replies, fn reply -> reply["number"] == number end) do
      %{"status" => status, "response" => response} -> {String.to_existing_atom(status), response}
      nil -> reply(socket, number, from + length(records))
    end
  end

  def config, do: Application.fetch_env!(:phoenix_durable_channel, __MODULE__)
end
