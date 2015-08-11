defmodule Rex.Mixfile do
  use Mix.Project

  def project do
    [app: :rex,
     version: "0.0.1",
     elixir: "~> 1.0",
     build_embedded: Mix.env in [:prod, :bench],
     start_permanent: Mix.env == :prod,
     test_coverage: [tool: Coverex.Task],
     deps: deps]
  end

  def application do
    [applications: [:logger]]
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
