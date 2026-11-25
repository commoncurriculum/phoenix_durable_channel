Application.put_env(:phoenix_durable_channel, PhoenixDurableChannel.Test.Endpoint,
  pubsub_server: PhoenixDurableChannel.Test.PubSub,
  server: false
)

defmodule PhoenixDurableChannel.Test.ClientChannel do
  use Phoenix.Channel
  use PhoenixDurableChannel.Channel, stream: PhoenixDurableChannel.Stream.Memory

  def join(_topic, _params, socket), do: {:ok, socket}

  def handle_in("run", _payload, socket) do
    send(socket.assigns.test, {:ran, socket.assigns.phx_number})
    {:reply, :ok, socket}
  end

  def handle_in("return", {:return, result}, socket) do
    result
    |> Tuple.to_list()
    |> Enum.map(fn
      :__socket__ -> socket
      element -> element
    end)
    |> List.to_tuple()
  end

  def handle_in("hibernate", _payload, socket), do: {:noreply, socket, :hibernate}
  def handle_in("retry", _payload, socket), do: {:retry, :busy, socket}
  def handle_in("crash", _payload, _socket), do: raise("crash requested by the test")

  def handle_info({:publish, event, payload}, socket) do
    :ok = push_durable(socket, event, payload)
    {:noreply, socket}
  end

  def handle_info(:timeout, socket) do
    send(socket.assigns.test, :timed_out)
    {:noreply, socket}
  end
end

defmodule PhoenixDurableChannel.Test.UserSocket do
  use Phoenix.Socket

  channel("client:*", PhoenixDurableChannel.Test.ClientChannel)

  def connect(_params, socket, _connect_info), do: {:ok, socket}
  def id(_socket), do: nil
end

defmodule PhoenixDurableChannel.Test.Endpoint do
  use Phoenix.Endpoint, otp_app: :phoenix_durable_channel
end

defmodule PhoenixDurableChannel.Test do
  import ExUnit.Assertions
  import Phoenix.ChannelTest

  alias PhoenixDurableChannel.Test.{ClientChannel, UserSocket}

  @endpoint PhoenixDurableChannel.Test.Endpoint

  def start do
    Supervisor.start_link(
      [{Phoenix.PubSub, name: PhoenixDurableChannel.Test.PubSub}, @endpoint],
      strategy: :one_for_one
    )
  end

  def topic, do: "client:#{System.unique_integer([:positive])}"

  def durable_join(topic \\ topic()) do
    {:ok, reply, socket} = client_join(topic, %{"phx_durable" => true})
    assert reply == %{phx_durable: true}
    assert_push("phx_reader", %{type: "phoenix"})
    socket
  end

  def plain_join(topic \\ topic()) do
    {:ok, reply, socket} = client_join(topic, %{})
    assert reply == %{}
    socket
  end

  def client_join(topic, params) do
    UserSocket
    |> socket(nil, %{test: self()})
    |> subscribe_and_join(ClientChannel, topic, params)
  end

  def received(socket) do
    ref = push(socket, "phx_sync", %{})
    assert_reply(ref, :ok, %{received: received})
    received
  end

  def idle(pid, attempts \\ 100) do
    case Process.info(pid, [:message_queue_len, :status, :current_function]) do
      nil ->
        :down

      [message_queue_len: 0, status: :waiting, current_function: {:erlang, :hibernate, 3}] ->
        :hibernated

      [message_queue_len: 0, status: :waiting, current_function: {:gen_server, _loop, _arity}] ->
        :waiting

      _busy when attempts > 0 ->
        Process.sleep(5)
        idle(pid, attempts - 1)
    end
  end
end
