defmodule PhoenixDurableChannel.Stream do
  @callback append(
              socket :: Phoenix.Socket.t(),
              event :: String.t(),
              payload :: map(),
              received :: non_neg_integer() | nil
            ) :: pos_integer()

  @callback received(socket :: Phoenix.Socket.t()) :: non_neg_integer()

  @callback count(
              socket :: Phoenix.Socket.t(),
              last :: non_neg_integer(),
              received :: non_neg_integer()
            ) :: pos_integer() | :moved

  @callback reader(socket :: Phoenix.Socket.t()) :: %{
              required(:type) => String.t(),
              required(:last) => non_neg_integer()
            }
end
