defmodule Redix.Mixfile do
  use Mix.Project

  @description "Fast, pipelined, resilient Redis driver for Elixir."

  @repo_url "https://github.com/whatyouhide/redix"

  @version "1.5.2"

  def project do
    [
      app: :redix,
      version: @version,
      elixir: "~> 1.12",
      build_embedded: Mix.env() == :prod,
      start_permanent: Mix.env() == :prod,
      deps: deps(),

      # Tests
      test_coverage: [tool: ExCoveralls],

      # Dialyzer
      dialyzer: [
        plt_local_path: "plts",
        plt_core_path: "plts"
      ],

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
          "CHANGELOG.md",
          "LICENSE.txt": [title: "License"]
        ]
      ]
    ]
  end

  def application do
    [extra_applications: [:logger, :ssl]]
  end

  defp package do
    [
      maintainers: ["Andrea Leopardi"],
      licenses: ["MIT"],
      links: %{"GitHub" => @repo_url, "Sponsor" => "https://github.com/sponsors/whatyouhide"}
    ]
  end

  defp deps do
    [
      {:telemetry, "~> 0.4.0 or ~> 1.0"},
      {:castore, "~> 0.1.0 or ~> 1.0", optional: true},
      {:nimble_options, "~> 0.5.0 or ~> 1.0"},

      # Dev and test dependencies
      {:dialyxir, "~> 1.4 and >= 1.4.2", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.28", only: :dev},
      {:excoveralls, "~> 0.17", only: :test},
      {:propcheck, "~> 1.1", only: :test},
      {:stream_data, "~> 1.1", only: [:dev, :test]}
    ]
  end
end
