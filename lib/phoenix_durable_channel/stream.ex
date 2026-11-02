defmodule PhoenixDurableChannel.Stream do
  @callback append(socket :: Phoenix.Socket.t(), event :: String.t(), payload :: map()) ::
              pos_integer()

  @callback reader(socket :: Phoenix.Socket.t()) :: %{
              required(:type) => String.t(),
              required(:last) => non_neg_integer()
            }

  @callback delivery() :: :channel | :reader
end
