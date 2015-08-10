defmodule ConnectionBench do
  use Benchfella

  before_each_bench _ do
    {:ok, recs} = Recs.start_link(host: "localhost", port: 6379)
    {:ok, redo} = :redo.start_link(:undefined, [])
    {:ok, eredis} = :eredis.start_link

    Recs.command(recs, ["SET", "k", "1"])

    {:ok, %{recs: recs, redo: redo, eredis: eredis}}
  end

  bench "command (Recs)", [recs: bench_context[:recs]] do
    Recs.command(recs, ~w(GET k))
  end

  bench "command (:redo)", [redo: bench_context[:redo]] do
    :redo.cmd(redo, ~w(GET k))
  end

  bench "command (:eredis)", [eredis: bench_context[:eredis]] do
    :eredis.q(eredis, ~w(GET k))
  end

  bench "pipeline (Recs)", [recs: bench_context[:recs], cmds: List.duplicate(~w(GET k), 1000)] do
    Recs.pipeline(recs, cmds)
  end

  bench "pipeline (:redo)", [redo: bench_context[:redo], cmds: List.duplicate(~w(GET k), 1000)] do
    :redo.cmd(redo, cmds)
  end

  bench "pipeline (:eredis)", [eredis: bench_context[:eredis], cmds: List.duplicate(~w(GET k), 1000)] do
    :eredis.qp(eredis, cmds)
  end

  bench "command (lots of them) (Recs)", [recs: bench_context[:recs]] do
    Enum.each 1..1000, fn(_) -> Recs.command(recs, ["GET", "k"]) end
  end

  bench "command (lots of them) (:redo)", [redo: bench_context[:redo]] do
    Enum.each 1..1000, fn(_) -> :redo.cmd(redo, ["GET", "k"]) end
  end
  bench "command (lots of them) (:eredis)", [eredis: bench_context[:eredis]] do
    Enum.each 1..1000, fn(_) -> :eredis.q(eredis, ["GET", "k"]) end
  end
end
