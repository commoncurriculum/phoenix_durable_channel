defmodule PhoenixDurableChannel.MixProject do
  use Mix.Project

  def project do
    [app: :phoenix_durable_channel, version: "0.1.0", elixir: "~> 1.17", deps: deps()]
  end

  def application, do: [extra_applications: [:logger, :crypto]]

  # s2_client is for PhoenixDurableChannel.Stream.S2, req for PhoenixDurableChannel.Stream.DurableStreams and
  # mongodb_driver for PhoenixDurableChannel.Stream.Mongo; an app adds the one for the store it uses.
  defp deps do
    [
      {:phoenix, "~> 1.7.21"},
      {:jason, "~> 1.4"},
      {:req, "~> 0.5", optional: true},
      {:mongodb_driver, "~> 1.5", optional: true},
      {:s2_client,
       git: "https://github.com/commoncurriculum/s2-elixir-client.git",
       ref: "b7f494de34c5297aa77ee97c31c61d120e6ce794",
       optional: true}
    ]
  end
end
