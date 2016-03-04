defmodule Redix.Mixfile do
  use Mix.Project

  @description """
  Superfast, pipelined, resilient Redis driver for Elixir.
  """

  @repo_url "https://github.com/whatyouhide/redix"

  @version "0.3.5"

  def project do
    [app: :redix,
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
     docs: [main: "Redix",
            source_ref: "v#{@version}",
            source_url: @repo_url,
            extras: ["README.md"]]]
  end

  def application do
    [applications: [:logger, :connection]]
  end

  defp package do
    [maintainers: ["Andrea Leopardi"],
     licenses: ["MIT"],
     links: %{"GitHub" => @repo_url}]
  end

  defp deps do
    [{:connection, "~> 1.0.0"},
     {:dialyze, "~> 0.2", only: :dev},
     {:benchfella, github: "alco/benchfella", only: :bench},
     {:redo, github: "heroku/redo", only: :bench},
     {:eredis, github: "wooga/eredis", only: :bench},
     {:yar, github: "dantswain/yar", only: :bench},
     {:coverex, "~> 1.4", only: :test},
     {:ex_doc, ">= 0.0.0", only: :docs}]
  end
end
