defmodule PhoenixDurableChannel.Stream do
  @moduledoc """
  Where a channel's durable messages to the client are kept, and how the client reads them.

  Messages are numbered 1, 2, 3 and so on, per channel topic. Each stream differs in where it
  keeps them and in its own read protocol; `reader/1` is what the client needs to read them,
  and it goes to the client in the join reply as `phx_reader`:

    * `%{type: "phoenix", epoch: ...}`: through the channel. The stream is also a
      `PhoenixDurableChannel.Stream.ThroughChannel`: `PhoenixDurableChannel.Stream.Memory`, or the app's own
      (a database).
    * `%{type: "s2", ...}`: from S2, with S2's own protocol. `PhoenixDurableChannel.Stream.S2`.
    * `%{type: "durable-streams", ...}`: from a Durable Streams server, with that protocol.
      `PhoenixDurableChannel.Stream.DurableStreams`.
  """

  @doc "Keeps a message for the client and returns its number."
  @callback append(socket :: Phoenix.Socket.t(), event :: String.t(), payload :: map()) ::
              pos_integer()

  @doc """
  What the client needs to read this channel's messages: `type` names the protocol, and
  `last` is the number of the last message kept now. A client with no number of its own
  reads after `last`, so what the channel appends after the join reaches it however late
  its read opens. It is called again when the client asks for a fresh read token.
  """
  @callback reader(socket :: Phoenix.Socket.t()) :: %{
              required(:type) => String.t(),
              required(:last) => non_neg_integer()
            }
end
