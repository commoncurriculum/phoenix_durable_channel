defmodule PhoenixDurableChannel.Stream.Mongo do
  @behaviour PhoenixDurableChannel.Stream
  @behaviour PhoenixDurableChannel.Stream.ThroughChannel

  @epoch "mongo"

  def create_indexes,
    do:
      :ok =
        Mongo.create_indexes(config()[:name], config()[:messages], [
          [key: [topic: 1, number: 1], name: "topic_number", unique: true],
          [
            key: [topic: 1, number: 1],
            name: "topic_number_counted",
            partialFilterExpression: %{received: %{"$type" => "number"}}
          ]
        ])

  @impl PhoenixDurableChannel.Stream
  def append(socket, event, payload, received) do
    number = last(socket.topic) + 1

    case insert(socket.topic, number, event, payload, received) do
      :ok ->
        number

      :taken ->
        append(socket, event, payload, received)

      {:error, error} ->
        raise "Mongo refused an append to #{socket.topic}: #{Exception.message(error)}"
    end
  end

  @impl PhoenixDurableChannel.Stream
  def count(socket, last, received) do
    case insert(socket.topic, last + 1, "phx_reply", %{received: received}, received) do
      :ok ->
        last + 1

      :taken ->
        :moved

      {:error, error} ->
        raise "Mongo refused the count of #{socket.topic}: #{Exception.message(error)}"
    end
  end

  defp insert(topic, number, event, payload, received) do
    document = %{topic: topic, number: number, event: event, payload: payload, received: received}

    case Mongo.insert_one(config()[:name], config()[:messages], document) do
      {:ok, _result} -> :ok
      {:error, %Mongo.WriteError{write_errors: [%{"code" => 11_000}]}} -> :taken
      {:error, %Mongo.Error{code: 11_000}} -> :taken
      {:error, error} -> {:error, error}
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
  def delivery, do: :channel

  @impl PhoenixDurableChannel.Stream
  def reader(socket), do: %{type: "phoenix", last: last(socket.topic), epoch: @epoch}

  @impl PhoenixDurableChannel.Stream
  def received(socket) do
    case newest(%{topic: socket.topic, received: %{"$type" => "number"}}, %{received: 1}) do
      %{"received" => received} -> received
      nil -> 0
    end
  end

  defp last(topic) do
    case newest(%{topic: topic}, %{number: 1}) do
      %{"number" => number} -> number
      nil -> 0
    end
  end

  defp newest(filter, projection),
    do:
      Mongo.find_one(config()[:name], config()[:messages], filter,
        sort: [number: -1],
        projection: projection
      )

  defp config, do: Application.fetch_env!(:phoenix_durable_channel, __MODULE__)
end
