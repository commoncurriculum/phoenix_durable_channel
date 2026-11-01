defmodule PhoenixDurableChannel.Serializer do
  @behaviour Phoenix.Socket.Serializer

  alias Phoenix.Socket.Message
  alias Phoenix.Socket.V2.JSONSerializer

  @impl true
  defdelegate fastlane!(broadcast), to: JSONSerializer

  @impl true
  def encode!(%Message{payload: {:durable, number, payload}} = message) do
    frame = [message.join_ref, message.ref, message.topic, message.event, payload, number]
    {:socket_push, :text, Phoenix.json_library().encode_to_iodata!(frame)}
  end

  def encode!(message), do: JSONSerializer.encode!(message)

  @impl true
  def decode!(raw, opts) do
    case opts[:opcode] == :text and Phoenix.json_library().decode!(raw) do
      [join_ref, ref, topic, event, payload, number] ->
        %Message{
          join_ref: join_ref,
          ref: ref,
          topic: topic,
          event: event,
          payload: {:durable, number, payload}
        }

      [join_ref, ref, topic, event, payload] ->
        %Message{join_ref: join_ref, ref: ref, topic: topic, event: event, payload: payload}

      false ->
        JSONSerializer.decode!(raw, opts)
    end
  end
end
