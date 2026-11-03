defmodule JepsenServer.MixProject do
  use Mix.Project

  def project do
    [app: :jepsen_server, version: "0.1.0", elixir: "~> 1.17", deps: deps()]
  end

  def application do
    [mod: {JepsenServer.Application, []}, extra_applications: [:logger]]
  end

  defp deps do
    [
      {:phoenix, "1.8.16"},
      {:phoenix_pubsub, "~> 2.4"},
      {:plug_cowboy, "2.7.5"},
      {:cowboy, "2.10.0", override: true},
      {:websock_adapter, "0.5.9"},
      {:jason, "~> 1.4"},
      {:mongodb_driver, "~> 1.5"},
      {:req, "~> 0.5"},
      {:phoenix_durable_channel, path: "../.."},
      {:s2_client,
       git: "https://github.com/commoncurriculum/s2-elixir-client.git",
       ref: "c3d82cc75391e10845acae72c0da2842d6e6263e"}
    ]
  end
end
