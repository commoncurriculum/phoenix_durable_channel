defmodule PhoenixDurableChannel.Stream.Mongo do
  @behaviour PhoenixDurableChannel.Stream
  @behaviour PhoenixDurableChannel.Stream.ThroughChannel

  @epoch "mongo"

  def create_indexes,
    do:
      :ok =
        Mongo.create_indexes(config()[:name], config()[:messages], [
          [key: [topic: 1, number: 1], name: "topic_number", unique: true]
        ])

  def find_newest(topic),
    do:
      Mongo.find_one(config()[:name], config()[:messages], %{topic: topic},
        sort: [number: -1],
        projection: %{number: 1, received: 1}
      ) || %{}

  def last(topic), do: find_newest(topic)["number"] || 0

  @impl PhoenixDurableChannel.Stream
  def append(socket, event, payload, received),
    do: append(socket, event, payload, received, last(socket.topic) + 1)

  def append(socket, event, payload, received, number) do
    document = %{
      topic: socket.topic,
      number: number,
      event: event,
      payload: payload,
      received: received
    }

    case Mongo.insert_one(config()[:name], config()[:messages], document) do
      {:ok, _result} ->
        number

      {:error, %Mongo.WriteError{write_errors: [%{"code" => 11_000}]}} ->
        append(socket, event, payload, received)

      {:error, %Mongo.Error{code: 11_000}} ->
        append(socket, event, payload, received)

      {:error, error} ->
        raise "Mongo refused an append to #{socket.topic}: #{Exception.message(error)}"
    end
  end

  @impl PhoenixDurableChannel.Stream.ThroughChannel
  def since(socket, number, limit) do
    latest = last(socket.topic)
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

  @impl PhoenixDurableChannel.Stream.ThroughChannel
  def epoch(_socket), do: @epoch

  @impl PhoenixDurableChannel.Stream
  def reader(socket), do: %{type: "phoenix", last: last(socket.topic), epoch: @epoch}

  @impl PhoenixDurableChannel.Stream
  def received(socket), do: find_newest(socket.topic)["received"] || 0

  def config, do: Application.fetch_env!(:phoenix_durable_channel, __MODULE__)
end
