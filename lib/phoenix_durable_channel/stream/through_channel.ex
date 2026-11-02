defmodule PhoenixDurableChannel.Stream.ThroughChannel do
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
