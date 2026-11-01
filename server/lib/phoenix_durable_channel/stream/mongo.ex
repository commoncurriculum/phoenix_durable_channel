defmodule PhoenixDurableChannel.Stream.Mongo do
  @moduledoc """
  A `PhoenixDurableChannel.Stream`, read through the channel, and a `PhoenixDurableChannel.Ledger`, in
  MongoDB.

  Each message is a document `{topic, number, event, payload}` in the `messages` collection,
  and each reply to the client's durable messages `{topic, number, status, response}` in
  `replies`. Both have a unique index on `{topic, number}`, which `create_indexes/0` makes:
  the index takes the number, so two appends never get the same one. Mongo keeps what it
  has, so the epoch never changes.

      config :phoenix_durable_channel, PhoenixDurableChannel.Stream.Mongo,
        name: MyApp.Mongo,                 # a `Mongo` topology
        messages: "phoenix_durable_channel_messages",
        replies: "phoenix_durable_channel_replies"
  """
  @behaviour PhoenixDurableChannel.Ledger
  @behaviour PhoenixDurableChannel.Stream
  @behaviour PhoenixDurableChannel.Stream.ThroughChannel

  @epoch "mongo"

  @doc "Makes the unique `{topic, number}` index on both collections."
  def create_indexes do
    Enum.each([config()[:messages], config()[:replies]], fn collection ->
      :ok =
        Mongo.create_indexes(config()[:name], collection, [
          [key: [topic: 1, number: 1], name: "topic_number", unique: true]
        ])
    end)
  end

  @doc "The number of the last document of `topic` in `collection`, 0 for none."
  def last(collection, topic) do
    document =
      Mongo.find_one(config()[:name], collection, %{topic: topic},
        sort: [number: -1],
        projection: %{number: 1}
      )

    (document || %{})["number"] || 0
  end

  @impl PhoenixDurableChannel.Stream
  def append(socket, event, payload),
    do: append(socket, event, payload, last(config()[:messages], socket.topic) + 1)

  def append(socket, event, payload, number) do
    document = %{topic: socket.topic, number: number, event: event, payload: payload}

    case Mongo.insert_one(config()[:name], config()[:messages], document) do
      {:ok, _result} ->
        number

      {:error, %Mongo.WriteError{write_errors: [%{"code" => 11_000}]}} ->
        append(socket, event, payload)

      {:error, %Mongo.Error{code: 11_000}} ->
        append(socket, event, payload)
    end
  end

  @impl PhoenixDurableChannel.Stream.ThroughChannel
  def since(socket, number, limit) do
    latest = last(config()[:messages], socket.topic)
    sent = min(latest, number + limit)

    messages =
      config()[:name]
      |> Mongo.find(
        config()[:messages],
        %{topic: socket.topic, number: %{"$gt" => number, "$lte" => sent}},
        sort: [number: 1]
      )
      |> Enum.map(fn document -> {document["number"], document["event"], document["payload"]} end)

    %{messages: messages, sent: sent, latest: latest, epoch: @epoch}
  end

  @impl PhoenixDurableChannel.Stream
  def reader(socket),
    do: %{type: "phoenix", last: last(config()[:messages], socket.topic), epoch: @epoch}

  @impl PhoenixDurableChannel.Ledger
  def received(socket), do: last(config()[:replies], socket.topic)

  # The unique index refuses a number already recorded, which changes nothing.
  @impl PhoenixDurableChannel.Ledger
  def put_received(socket, number, {status, response}) do
    document = %{topic: socket.topic, number: number, status: status, response: response}

    case Mongo.insert_one(config()[:name], config()[:replies], document) do
      {:ok, _result} -> :ok
      {:error, %Mongo.WriteError{write_errors: [%{"code" => 11_000}]}} -> :ok
      {:error, %Mongo.Error{code: 11_000}} -> :ok
    end
  end

  @impl PhoenixDurableChannel.Ledger
  def reply(socket, number) do
    document =
      Mongo.find_one(config()[:name], config()[:replies], %{topic: socket.topic, number: number})

    {String.to_existing_atom(document["status"]), document["response"]}
  end

  def config, do: Application.fetch_env!(:phoenix_durable_channel, __MODULE__)
end
