defmodule PhoenixDurableChannel.MixProject do
  use Mix.Project

  def project do
    [app: :phoenix_durable_channel, version: "0.1.0", elixir: "~> 1.17", deps: deps()]
  end

  def application do
    [extra_applications: [:logger, :crypto] ++ if(Mix.env() == :test, do: [:inets], else: [])]
  end

  defp deps do
    [
      {:phoenix, "~> 1.7.21 or ~> 1.8"},
      {:jason, "~> 1.4"},
      {:req, "~> 0.5", optional: true},
      {:mongodb_driver, "~> 1.5", optional: true},
      {:s2_client,
       git: "https://github.com/commoncurriculum/s2-elixir-client.git",
       ref: "b7f494de34c5297aa77ee97c31c61d120e6ce794",
       optional: true},
      {:plug_cowboy, "~> 2.7", only: :test},
      {:cowlib, "~> 2.15.0", only: :test},
      {:ranch, "~> 2.1.0", only: :test},
      {:bandit, "~> 1.0", only: :test},
      {:mint_web_socket, "~> 1.0.0", only: :test}
    ]
  end
end
