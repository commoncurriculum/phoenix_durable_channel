Application.ensure_all_started(:plug_cowboy)

{:ok, _} = PhoenixDurableChannel.Stream.Memory.start_link([])

assert_timeout = String.to_integer(System.get_env("ELIXIR_ASSERT_TIMEOUT") || "200")
ExUnit.start(assert_receive_timeout: assert_timeout)
