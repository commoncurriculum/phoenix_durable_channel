defmodule PhoenixDurableChannel.FramesTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  @moduletag :capture_log

  import Phoenix.ChannelTest
  import PhoenixDurableChannel.Test

  alias Phoenix.Socket.{Message, Reply}
  alias Phoenix.Socket.V2.JSONSerializer
  alias PhoenixDurableChannel.Serializer

  property "a text frame decodes as Phoenix's V2 serializer decodes it, but for a sixth slot's number" do
    check all(frame <- json(), max_runs: 500) do
      raw = Jason.encode!(frame)

      case frame do
        [join_ref, ref, topic, event, payload, number] ->
          assert Serializer.decode!(raw, opcode: :text) == %Message{
                   join_ref: join_ref,
                   ref: ref,
                   topic: topic,
                   event: event,
                   payload: {:durable, number, payload}
                 }

        _other ->
          assert decoded(Serializer, raw) == decoded(JSONSerializer, raw)
      end
    end
  end

  property "the server runs only the next number, counts only what it ran, and answers every other frame" do
    check all(
            numbers <- list_of(one_of([constant(:next), integer(-2..6), json()]), max_length: 12),
            max_runs: 100
          ) do
      socket = durable_join()

      expected =
        Enum.reduce(numbers, 0, fn number, received ->
          number = if number == :next, do: received + 1, else: number
          raw = Jason.encode!([socket.join_ref, "1", socket.topic, "run", %{}, number])
          message = Serializer.decode!(raw, opcode: :text)
          ref = push(socket, message.event, message.payload)

          case number do
            number when is_integer(number) and number == received + 1 ->
              assert_receive {:ran, ^number}
              assert_push("phx_reply", {:durable, _, _, %{number: ^number, status: :ok}})
              number

            number when is_integer(number) and number > received + 1 ->
              assert_reply(ref, :error, %{phx_not_taken: "out_of_order"})
              received

            number when is_integer(number) and number >= 1 ->
              assert received(socket) == received
              received

            _invalid ->
              assert_reply(ref, :error, %{reason: "invalid"})
              received
          end
        end)

      assert received(socket) == expected
      refute_received {:ran, _number}
      refute_received %Reply{}
    end
  end

  defp decoded(serializer, raw) do
    serializer.decode!(raw, opcode: :text)
  rescue
    _error -> :raised
  end

  defp json do
    leaf =
      one_of([
        constant(nil),
        boolean(),
        integer(),
        float(),
        string(:printable, max_length: 6)
      ])

    tree(leaf, fn json ->
      one_of([
        list_of(json, max_length: 8),
        map_of(string(:alphanumeric, max_length: 4), json, max_length: 3)
      ])
    end)
  end
end
