defmodule Redix.Mixfile do
  use Mix.Project

  @description """
  Superfast, pipelined, resilient Redis driver for Elixir.
  """

  @version "0.1.0-dev"

  def project do
    [
      app: :redix,
      version: @version,
      elixir: "~> 1.0",
      build_embedded: Mix.env in [:prod, :bench],
      start_permanent: Mix.env == :prod,
      test_coverage: [tool: Coverex.Task],
      deps: deps,

      # Hex
      package: package,
      description: @description,

      # Docs
      name: "Redix",
      source_url: "https://github.com/whatyouhide/redix",
    ]
  end

  def application do
    [applications: [:logger, :connection]]
  end

  defp package do
    [
      contributors: "Andrea Leopardi",
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/whatyouhide/redix"},
    ]
  end

  defp deps do
    [
      {:connection, "~> 1.0.0-rc.1"},
      {:dialyze, "~> 0.2", only: :dev},
      {:benchfella, github: "alco/benchfella", only: :bench},
      {:redo, github: "heroku/redo", only: :bench},
      {:eredis, github: "wooga/eredis", only: :bench},
      {:coverex, "~> 1.4", only: :test},
      {:ex_doc, ">= 0.0.0", only: :docs},
    ]
  end
end
