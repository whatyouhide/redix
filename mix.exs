defmodule Redix.Mixfile do
  use Mix.Project

  @description "Superfast, pipelined, resilient Redis driver for Elixir."

  @repo_url "https://github.com/whatyouhide/redix"

  @version "0.6.1"

  def project() do
    [app: :redix,
     version: @version,
     elixir: "~> 1.2",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     deps: deps(),

     # Hex
     package: package(),
     description: @description,

     # Docs
     name: "Redix",
     docs: [main: "Redix",
            source_ref: "v#{@version}",
            source_url: @repo_url,
            extras: ["README.md", "pages/Reconnections.md", "pages/Real-world usage.md"]]]
  end

  def application() do
    [applications: [:logger, :connection]]
  end

  defp package() do
    [maintainers: ["Andrea Leopardi", "Aleksei Magusev"],
     licenses: ["MIT"],
     links: %{"GitHub" => @repo_url}]
  end

  defp deps() do
    [{:connection, "~> 1.0"},
     {:ex_doc, "~> 0.15", only: :dev}]
  end
end
