defmodule PhoenixDurableChannel.Stream.ThroughChannel do
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

  @callback since(
              socket :: Phoenix.Socket.t(),
              number :: non_neg_integer(),
              limit :: pos_integer()
            ) ::
              page()
end
