defmodule TestServer.MixProject do
  use Mix.Project

  def project do
    [app: :test_server, version: "0.1.0", elixir: "~> 1.17", start_permanent: true, deps: deps()]
  end

  def application do
    [mod: {TestServer.Application, []}, extra_applications: [:logger]]
  end

  defp deps do
    [
      {:phoenix, "1.8.16"},
      {:phoenix_pubsub, "~> 2.4"},
      {:plug_cowboy, "2.7.5"},
      {:cowboy, "2.10.0", override: true},
      {:websock_adapter, "0.5.9"},
      {:jason, "~> 1.4"},
      {:phoenix_durable_channel, path: "../.."},
      {:req, "~> 0.5"},
      {:mongodb_driver, "~> 1.5"},
      {:s2_client,
       git: "https://github.com/commoncurriculum/s2-elixir-client.git",
       ref: "b7f494de34c5297aa77ee97c31c61d120e6ce794"}
    ]
  end
end
