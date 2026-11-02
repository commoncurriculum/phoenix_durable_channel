defmodule PhoenixDurableChannel.NoCommentsTest do
  use ExUnit.Case, async: true

  test "no Elixir file this repo owns has a comment or a doc" do
    commented =
      "**/*.{ex,exs}"
      |> Path.wildcard()
      |> Enum.reject(
        &String.match?(&1, ~r{(^|/)(deps|_build|node_modules)/|^test/(phoenix|support)/})
      )
      |> Enum.filter(fn path ->
        case path |> File.read!() |> Code.string_to_quoted_with_comments!() do
          {_quoted, [_ | _]} ->
            true

          {quoted, []} ->
            quoted
            |> Macro.prewalk(false, fn
              {:@, _, [{attribute, _, [text]}]} = node, _found
              when attribute in [:doc, :moduledoc, :typedoc] and is_binary(text) ->
                {node, true}

              node, found ->
                {node, found}
            end)
            |> elem(1)
        end
      end)

    assert commented == []
  end
end
