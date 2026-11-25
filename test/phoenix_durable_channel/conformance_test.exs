defmodule PhoenixDurableChannel.ConformanceTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  @moduletag :capture_log

  import Phoenix.ChannelTest
  import PhoenixDurableChannel.Test

  alias Phoenix.Socket.{Message, Reply}

  test "the returns below are every handle_in/3 return Phoenix's Channel.Server accepts" do
    assert Map.keys(returns()) |> MapSet.new() == phoenix_returns()
  end

  property "a durable push's handler return behaves as it does on a plain channel, answered in the stream" do
    Process.flag(:trap_exit, true)

    check all(
            shape <- member_of(Map.keys(returns())),
            result <- returns()[shape],
            max_runs: 200
          ) do
      plain = observe(plain_join(), {:return, result})
      durable = observe(durable_join(), {:durable, 1, {:return, result}})

      assert durable.process == plain.process
      assert durable.timed_out == plain.timed_out
      assert durable.answer == (plain.reply || {:ok, %{}})

      case shape do
        {:stop, _arity} -> assert durable.reply == (plain.reply || {:ok, %{}})
        _other -> assert durable.reply == nil
      end
    end
  end

  defp returns do
    %{
      {:reply, 3} => tuple({constant(:reply), reply(), constant(:__socket__)}),
      {:noreply, 2} => constant({:noreply, :__socket__}),
      {:noreply, 3} =>
        tuple({constant(:noreply), constant(:__socket__), member_of([0, :hibernate])}),
      {:stop, 3} => tuple({constant(:stop), reason(), constant(:__socket__)}),
      {:stop, 4} => tuple({constant(:stop), reason(), reply(), constant(:__socket__)})
    }
  end

  defp status, do: one_of([member_of([:ok, :error]), atom(:alphanumeric)])

  defp reply do
    one_of([
      status(),
      tuple({status(), map_of(string(:alphanumeric, max_length: 4), integer(), max_length: 3)})
    ])
  end

  defp reason do
    one_of([
      member_of([:normal, :shutdown]),
      tuple({constant(:shutdown), atom(:alphanumeric)}),
      atom(:alphanumeric)
    ])
  end

  defp observe(socket, payload) do
    pid = socket.channel_pid
    monitor = Process.monitor(pid)
    ref = push(socket, "return", payload)

    process =
      case idle(pid) do
        :down ->
          assert_receive {:DOWN, ^monitor, :process, ^pid, reason}
          {:down, reason}

        alive ->
          Process.demonitor(monitor, [:flush])
          alive
      end

    messages = drain()

    %{
      process: process,
      timed_out: :timed_out in messages,
      reply:
        Enum.find_value(messages, fn
          %Reply{ref: ^ref, status: status, payload: response} -> {status, response}
          _other -> nil
        end),
      answer:
        Enum.find_value(messages, fn
          %Message{event: "phx_reply", payload: {:durable, _, _, %{number: 1} = answer}} ->
            {answer.status, answer.response}

          _other ->
            nil
        end)
    }
  end

  defp drain(messages \\ []) do
    receive do
      message -> drain([message | messages])
    after
      0 -> Enum.reverse(messages)
    end
  end

  defp phoenix_returns do
    Mix.Project.deps_paths()
    |> Map.fetch!(:phoenix)
    |> Path.join("lib/phoenix/channel/server.ex")
    |> File.read!()
    |> Code.string_to_quoted!()
    |> Macro.prewalk(MapSet.new(), fn
      {:defp, _, [{:handle_in, _, [result]}, _]} = node, returns ->
        {node, shape(result, returns)}

      {:defp, _, [{:handle_result, _, [result, callback]}, _]} = node, returns
      when callback != :handle_call ->
        {node, shape(result, returns)}

      node, returns ->
        {node, returns}
    end)
    |> elem(1)
  end

  defp shape({:{}, _, [tag | rest]}, returns) when is_atom(tag),
    do: MapSet.put(returns, {tag, length(rest) + 1})

  defp shape({tag, _socket}, returns) when is_atom(tag), do: MapSet.put(returns, {tag, 2})
  defp shape(_variable, returns), do: returns
end
