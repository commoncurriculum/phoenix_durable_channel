defmodule PhoenixDurableChannel.Stream.ThroughChannel do
  @moduledoc """
  A `PhoenixDurableChannel.Stream` the client reads through the channel, `%{type: "phoenix"}`. The
  channel pushes each message as it is appended, and answers the client's `phx_read` with
  what the stream kept after the client's last message.

  Its `reader/1` also carries `epoch`, which names the numbering: a stream that loses what it
  kept and numbers from 1 again (a node's memory after a restart) starts a new epoch, so a
  client reading after a number of the old one reads the new one from the start instead.
  An epoch never changes while a channel process of the topic is alive.
  """

  @typedoc """
  A page of the messages kept after a number: `messages` in order, `sent` the last number
  the page covers (a number whose message is gone is covered without one), `latest` the
  stream's last number, and `epoch` its numbering.
  """
  @type page :: %{
          messages: [{pos_integer(), String.t(), map()}],
          sent: non_neg_integer(),
          latest: non_neg_integer(),
          epoch: String.t()
        }

  @doc """
  The messages kept after `number`, at most `limit` numbers of them. `sent` and `latest` are
  read together with them, so none is appended in between.
  """
  @callback since(
              socket :: Phoenix.Socket.t(),
              number :: non_neg_integer(),
              limit :: pos_integer()
            ) ::
              page()
end
