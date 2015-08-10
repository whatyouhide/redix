defmodule Recs.Mixfile do
  use Mix.Project

  def project do
    [app: :recs,
     version: "0.0.1",
     elixir: "~> 1.0",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     deps: deps]
  end

  def application do
    [applications: [:logger]]
  end

  defp deps do
    [
      {:benchfella, github: "alco/benchfella", only: :dev},
      {:redo, github: "heroku/redo", only: :dev}, # just for benchmarks
    ]
  end
end
