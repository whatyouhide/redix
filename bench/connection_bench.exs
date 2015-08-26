defmodule ConnectionBench do
  use Benchfella

  before_each_bench _ do
    {:ok, redix} = Redix.start_link(host: "localhost", port: 6379)
    {:ok, redo} = :redo.start_link(:undefined, [])
    {:ok, eredis} = :eredis.start_link
    {:ok, yar} = YAR.connect("localhost", 6379)

    Redix.command(redix, ["SET", "k", "1"])

    context = %{
      redix: redix,
      redo: redo,
      eredis: eredis,
      yar: yar,
      pipeline_cmds_lots: List.duplicate(~w(GET k), 10_000),
      pipeline_cmds_few: List.duplicate(~w(GET k), 5),
    }

    {:ok, context}
  end

  bench "[Redix] single command (GET)", [redix: bench_context[:redix]] do
    Redix.command(redix, ~w(GET k))
  end
  bench "[:redo] single command (GET)", [redo: bench_context[:redo]] do
    :redo.cmd(redo, ~w(GET k))
  end
  bench "[:eredis] single command (GET)", [eredis: bench_context[:eredis]] do
    :eredis.q(eredis, ~w(GET k))
  end
  bench "[YAR] single command (GET)", [yar: bench_context[:yar]] do
    YAR.execute(yar, ~w(GET k))
  end

  bench "[Redix] few pipelined commands", [redix: bench_context[:redix], cmds: bench_context[:pipeline_cmds_few]] do
    Redix.pipeline(redix, cmds)
  end
  bench "[:redo] few pipelined commands", [redo: bench_context[:redo], cmds: bench_context[:pipeline_cmds_few]] do
    :redo.cmd(redo, cmds)
  end
  bench "[:eredis] few pipelined commands", [eredis: bench_context[:eredis], cmds: bench_context[:pipeline_cmds_few]] do
    :eredis.qp(eredis, cmds)
  end
  bench "[YAR] few pipelined commands", [yar: bench_context[:yar], cmds: bench_context[:pipeline_cmds_few]] do
    YAR.pipeline(yar, cmds)
  end

  bench "[Redix] lots of pipelined commands", [redix: bench_context[:redix], cmds: bench_context[:pipeline_cmds_lots]] do
    Redix.pipeline(redix, cmds)
  end
  bench "[:redo] lots of pipelined commands", [redo: bench_context[:redo], cmds: bench_context[:pipeline_cmds_lots]] do
    :redo.cmd(redo, cmds)
  end
  bench "[:eredis] lots of pipelined commands", [eredis: bench_context[:eredis], cmds: bench_context[:pipeline_cmds_lots]] do
    :eredis.qp(eredis, cmds)
  end
  bench "[YAR] lots of pipelined commands", [yar: bench_context[:yar], cmds: bench_context[:pipeline_cmds_lots]] do
    YAR.pipeline(yar, cmds)
  end

  bench "[Redix] lots of commands, one after the other", [redix: bench_context[:redix]] do
    Enum.each 1..1000, fn(_) -> Redix.command(redix, ["GET", "k"]) end
  end
  bench "[:redo] lots of commands, one after the other", [redo: bench_context[:redo]] do
    Enum.each 1..1000, fn(_) -> :redo.cmd(redo, ["GET", "k"]) end
  end
  bench "[:eredis] lots of commands, one after the other", [eredis: bench_context[:eredis]] do
    Enum.each 1..1000, fn(_) -> :eredis.q(eredis, ["GET", "k"]) end
  end
  bench "[YAR] lots of commands, one after the other", [yar: bench_context[:yar]] do
    Enum.each 1..1000, fn(_) -> YAR.execute(yar, ["GET", "k"]) end
  end
end
