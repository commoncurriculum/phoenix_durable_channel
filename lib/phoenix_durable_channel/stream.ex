defmodule PhoenixDurableChannel.Stream do
  @callback append(
              socket :: Phoenix.Socket.t(),
              event :: String.t(),
              payload :: map(),
              received :: non_neg_integer()
            ) :: pos_integer()

  @callback received(socket :: Phoenix.Socket.t()) :: non_neg_integer()

  @callback reader(socket :: Phoenix.Socket.t()) :: %{
              required(:type) => String.t(),
              required(:last) => non_neg_integer()
            }
end
