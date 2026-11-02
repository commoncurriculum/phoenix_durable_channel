defmodule PhoenixDurableChannel.Ledger do
  @type reply :: {atom(), map()}

  @callback received(socket :: Phoenix.Socket.t()) :: non_neg_integer()

  @callback put_received(socket :: Phoenix.Socket.t(), number :: pos_integer(), reply()) :: :ok

  @callback reply(socket :: Phoenix.Socket.t(), number :: pos_integer()) :: reply() | nil
end
