defmodule Redix.Mixfile do
  use Mix.Project

  @description "Fast, pipelined, resilient Redis driver for Elixir."

  @repo_url "https://github.com/whatyouhide/redix"

  @version "0.9.3"

  def project() do
    [
      app: :redix,
      version: @version,
      elixir: "~> 1.6",
      build_embedded: Mix.env() == :prod,
      start_permanent: Mix.env() == :prod,
      deps: deps(),

      # Hex
      package: package(),
      description: @description,

      # Docs
      name: "Redix",
      docs: [
        main: "Redix",
        source_ref: "v#{@version}",
        source_url: @repo_url,
        extras: [
          "README.md",
          "pages/Reconnections.md",
          "pages/Real-world usage.md",
          "pages/Telemetry.md",
          "CHANGELOG.md"
        ]
      ]
    ]
  end

  def application() do
    [extra_applications: [:logger, :ssl]]
  end

  defp package() do
    [
      maintainers: ["Andrea Leopardi", "Aleksei Magusev"],
      licenses: ["MIT"],
      links: %{"GitHub" => @repo_url}
    ]
  end

  defp deps() do
    [
      {:telemetry, "~> 0.4.0"},
      {:ex_doc, "~> 0.19", only: :dev},
      {:dialyxir, "~> 1.0.0-rc.6", only: :dev},
      {:stream_data, "~> 0.4", only: :test},
      {:propcheck, "~> 1.1", only: :test}
    ]
  end
end
