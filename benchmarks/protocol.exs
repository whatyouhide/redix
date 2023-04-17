Mix.install([
  {:redix, path: "."},
  {:benchee, "~> 1.1"},
  {:benchee_html, "~> 1.0"},
  {:benchee_markdown, "~> 0.3"},
  {:eredis, "~> 1.7"}
])

defmodule Helpers do
  def parse_with_continuations([data | datas], cont \\ &Redix.Protocol.parse/1) do
    case cont.(data) do
      {:continuation, new_cont} -> parse_with_continuations(datas, new_cont)
      {:ok, value, rest} -> {:ok, value, rest}
    end
  end
end

Benchee.run(
  %{
    "Parse a bulk string split into 1kb chunks" => fn %{chunks: datas} ->
      {:ok, _value, _rest} = Helpers.parse_with_continuations(datas)
    end
  },
  # Inputs are expressed in number of 1kb chunks
  inputs: %{
    "1 Kb" => 1,
    "1 Mb" => 1024,
    "70 Mb" => 70 * 1024
  },
  before_scenario: fn chunks_of_1kb ->
    chunks = for _ <- 1..chunks_of_1kb, do: :crypto.strong_rand_bytes(1024)
    total_size = chunks_of_1kb * 1024
    chunks = ["$#{total_size}\r\n" | chunks] ++ ["\r\n"]

    %{chunks: chunks}
  end,
  save: [path: "redix-main.benchee", tag: "main"]
)
