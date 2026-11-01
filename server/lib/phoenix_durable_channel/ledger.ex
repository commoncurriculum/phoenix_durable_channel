defmodule PhoenixDurableChannel.Ledger do
  @moduledoc """
  Where a channel keeps what the client's durable messages need, so each is handled in order
  and a resend gets the reply already given. The server's own durable messages go to a
  `PhoenixDurableChannel.Stream`.

  The count only works when it is keyed by one client (a session id in `socket.assigns`,
  or a topic that names one client): two clients sharing a count would each have the
  other's messages answered as already handled. A client that loads again (a page reload)
  is another client, and needs a key of its own.

  The client never has more than 32 durable messages unanswered, so a ledger keeps at least
  the last 32 replies: a resend is always of one of them.
  """

  @type reply :: {atom(), map()}

  @doc "The number of the last durable message handled from the client, 0 for none."
  @callback received(socket :: Phoenix.Socket.t()) :: non_neg_integer()

  @doc """
  Records that the client's durable message `number` was handled, and its reply. The count
  never goes back: recording a number below it keeps the count.
  """
  @callback put_received(socket :: Phoenix.Socket.t(), number :: pos_integer(), reply()) :: :ok

  @doc "The reply recorded for `number`, one of the last 32 recorded."
  @callback reply(socket :: Phoenix.Socket.t(), number :: pos_integer()) :: reply()
end
