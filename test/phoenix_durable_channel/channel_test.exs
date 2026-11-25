defmodule PhoenixDurableChannel.ChannelTest do
  use ExUnit.Case, async: true

  @moduletag :capture_log

  import ExUnit.CaptureLog
  import Phoenix.ChannelTest
  import PhoenixDurableChannel.Test

  alias PhoenixDurableChannel.Test.ClientChannel

  setup do
    topic = topic()
    %{socket: durable_join(topic), topic: topic}
  end

  def forward(event, _measurements, metadata, test), do: send(test, {:telemetry, event, metadata})

  test "a durable push runs once, and a resend of it is not run again", %{
    socket: socket,
    topic: topic
  } do
    :telemetry.attach(topic, [:phoenix_durable_channel, :push], &__MODULE__.forward/4, self())
    on_exit(fn -> :telemetry.detach(topic) end)

    push(socket, "run", {:durable, 1, %{}})
    assert_receive {:ran, 1}
    assert_push("phx_reply", {:durable, 2, _epoch, %{number: 1, status: :ok, response: %{}}})
    assert_receive {:telemetry, _, %{topic: ^topic, number: 1, outcome: :ok}}

    push(socket, "run", {:durable, 1, %{}})
    assert_receive {:telemetry, _, %{topic: ^topic, number: 1, received: 1, outcome: :old}}
    refute_received {:ran, 1}
    assert received(socket) == 1
  end

  test "a push past the next number is not taken", %{socket: socket} do
    ref = push(socket, "run", {:durable, 2, %{}})
    assert_reply(ref, :error, %{phx_not_taken: "out_of_order"})
    refute_received {:ran, _number}
    assert received(socket) == 0
  end

  test "a push numbered with anything but a positive integer is invalid", %{socket: socket} do
    for number <- [0, -1, "1", 1.0, nil] do
      ref = push(socket, "run", {:durable, number, %{}})
      assert_reply(ref, :error, %{reason: "invalid"})
    end

    refute_received {:ran, _number}
    assert received(socket) == 0
  end

  test "a push the handler can't take now is answered with its reason and not counted", %{
    socket: socket
  } do
    ref = push(socket, "retry", {:durable, 1, %{}})
    assert_reply(ref, :error, %{phx_not_taken: :busy})
    assert received(socket) == 0
  end

  test "a handler that raises is answered crashed and the channel stays up", %{socket: socket} do
    log =
      capture_log(fn ->
        push(socket, "crash", {:durable, 1, %{}})

        assert_push(
          "phx_reply",
          {:durable, 2, _epoch, %{number: 1, status: :error, response: %{reason: "crashed"}}}
        )
      end)

    assert log =~ "crash requested by the test"
    assert Process.alive?(socket.channel_pid)
    assert received(socket) == 1
  end

  test "a handler's hibernate is kept after its answer is appended", %{socket: socket} do
    push(socket, "hibernate", {:durable, 1, %{}})
    assert_push("phx_reply", {:durable, 2, _epoch, %{number: 1, status: :ok}})
    assert idle(socket.channel_pid) == :hibernated
  end

  test "push_durable/3 refuses phx_reply", %{socket: socket} do
    assert_raise ArgumentError, ~r/can't push "phx_reply"/, fn ->
      ClientChannel.push_durable(socket, "phx_reply", %{})
    end
  end

  test "phx_read resends what the stream kept after a number, then replies where it got to", %{
    socket: socket
  } do
    for n <- 1..3, do: send(socket.channel_pid, {:publish, "news", %{n: n}})

    assert_push("news", {:durable, 2, epoch, %{n: 1}})
    assert_push("news", {:durable, 3, ^epoch, %{n: 2}})
    assert_push("news", {:durable, 4, ^epoch, %{n: 3}})

    ref = push(socket, "phx_read", %{"after" => 2})
    assert_push("news", {:durable, 3, ^epoch, %{n: 2}})
    assert_push("news", {:durable, 4, ^epoch, %{n: 3}})
    assert_reply(ref, :ok, %{sent: 4, epoch: ^epoch})
  end

  test "a second durable join on the topic replaces the first", %{socket: first, topic: topic} do
    Process.flag(:trap_exit, true)
    replaced = first.channel_pid

    {:ok, _reply, _second} = client_join(topic, %{"phx_durable" => true})

    assert_push("phx_error", %{})
    assert_receive {:EXIT, ^replaced, {:shutdown, :replaced}}
    assert_push("phx_reader", %{last: 1})
  end

  test "a channel joined without phx_durable refuses durable frames and durable events" do
    plain = plain_join()

    for {event, payload} <- [{"run", {:durable, 1, %{}}}, {"phx_sync", %{}}, {"phx_reader", %{}}] do
      ref = push(plain, event, payload)
      assert_reply(ref, :error, %{reason: "invalid"})
    end

    refute_received {:ran, _number}
  end
end
