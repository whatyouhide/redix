defmodule ConnectionBench do
  use Benchfella

  before_each_bench _ do
    {:ok, recs} = Red.start_link(host: "localhost", port: 6379)
    {:ok, redo} = :redo.start_link(:undefined, [])
    {:ok, eredis} = :eredis.start_link

    Red.command(recs, ["SET", "k", "1"])

    context = %{
      recs: recs,
      redo: redo,
      eredis: eredis,
      pipeline_cmds_lots: List.duplicate(~w(GET k), 10_000),
      pipeline_cmds_few: List.duplicate(~w(GET k), 5),
    }

    {:ok, context}
  end

  bench "[Red] single command (GET)", [recs: bench_context[:recs]] do
    Red.command(recs, ~w(GET k))
  end
  bench "[:redo] single command (GET)", [redo: bench_context[:redo]] do
    :redo.cmd(redo, ~w(GET k))
  end
  bench "[:eredis] single command (GET)", [eredis: bench_context[:eredis]] do
    :eredis.q(eredis, ~w(GET k))
  end

  bench "[Recs] few pipelined commands", [recs: bench_context[:recs], cmds: bench_context[:pipeline_cmds_few]] do
    Red.pipeline(recs, cmds)
  end
  bench "[:redo] few pipelined commands", [redo: bench_context[:redo], cmds: bench_context[:pipeline_cmds_few]] do
    :redo.cmd(redo, cmds)
  end
  bench "[:eredis] few pipelined commands", [eredis: bench_context[:eredis], cmds: bench_context[:pipeline_cmds_few]] do
    :eredis.qp(eredis, cmds)
  end

  bench "[Red] lots of pipelined commands", [recs: bench_context[:recs], cmds: bench_context[:pipeline_cmds_lots]] do
    Red.pipeline(recs, cmds)
  end
  bench "[:redo] lots of pipelined commands", [redo: bench_context[:redo], cmds: bench_context[:pipeline_cmds_lots]] do
    :redo.cmd(redo, cmds)
  end
  bench "[:eredis] lots of pipelined commands", [eredis: bench_context[:eredis], cmds: bench_context[:pipeline_cmds_lots]] do
    :eredis.qp(eredis, cmds)
  end

  bench "[Red] lots of commands, one after the other", [recs: bench_context[:recs]] do
    Enum.each 1..1000, fn(_) -> Red.command(recs, ["GET", "k"]) end
  end
  bench "[:redo] lots of commands, one after the other", [redo: bench_context[:redo]] do
    Enum.each 1..1000, fn(_) -> :redo.cmd(redo, ["GET", "k"]) end
  end
  bench "[:eredis] lots of commands, one after the other", [eredis: bench_context[:eredis]] do
    Enum.each 1..1000, fn(_) -> :eredis.q(eredis, ["GET", "k"]) end
  end
end
