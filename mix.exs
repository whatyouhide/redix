defmodule Redix.Mixfile do
  use Mix.Project

  @description "Fast, pipelined, resilient Redis driver for Elixir."

  @repo_url "https://github.com/whatyouhide/redix"

  @version "1.1.5"

  def project() do
    [
      app: :redix,
      version: @version,
      elixir: "~> 1.8",
      build_embedded: Mix.env() == :prod,
      start_permanent: Mix.env() == :prod,
      deps: deps(),

      # Tests
      test_coverage: [tool: ExCoveralls],

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
      maintainers: ["Andrea Leopardi"],
      licenses: ["MIT"],
      links: %{"GitHub" => @repo_url}
    ]
  end

  defp deps() do
    maybe_propcheck =
      if Version.match?(System.version(), "~> 1.11") do
        [{:propcheck, "~> 1.1", only: :test}]
      else
        []
      end

    [
      {:telemetry, "~> 0.4.0 or ~> 1.0"},
      {:castore, "~> 0.1.0", optional: true},

      # Dev and test dependencies
      {:dialyxir, "~> 1.0.0-rc.6", only: :dev, runtime: false},
      {:ex_doc, "~> 0.28", only: :dev},
      {:excoveralls, "~> 0.14", only: :test},
      {:stream_data, "~> 0.4", only: [:dev, :test]}
    ] ++ maybe_propcheck
  end
end
