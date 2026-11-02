defmodule PhoenixDurableChannel.NoCommentsTest do
  use ExUnit.Case, async: true

  test "no Elixir file this repo owns has a comment" do
    commented =
      ["lib/**/*.ex", "integration_test/phoenix_app/lib/*.ex", "jepsen/phoenix_app/lib/*.ex"]
      |> Enum.flat_map(&Path.wildcard/1)
      |> Enum.filter(fn path ->
        path |> File.read!() |> Code.string_to_quoted_with_comments!() |> elem(1) != []
      end)

    assert commented == []
  end
end
