defmodule Redix.ClusterPropertiesTest do
  use ExUnit.Case

  use PropCheck.StateM
  use PropCheck

  @moduletag :cluster
  @moduletag :propcheck
  @moduletag :capture_log

  @nodes ["redis://localhost:7000", "redis://localhost:7001", "redis://localhost:7002"]

  # All cluster ports (3 primaries + 6 replicas). We flush every node between
  # runs; replicas reject writes with READONLY, which we ignore.
  @ports 7000..7008

  @cluster :redix_cluster_property_test

  # A pool of base keys that intentionally hash to *different* slots, so the
  # model exercises cross-node routing and transparent pipeline splitting rather
  # than always hitting a single node.
  @keys ~w(alpha beta gamma delta epsilon zeta eta theta iota kappa)

  # Thin wrapper around the public API. Functions return the raw `{:ok, _}` /
  # `{:error, _}` tuples so postconditions can assert on the exact shape.
  defmodule C do
    @cluster :redix_cluster_property_test

    def set(key, value), do: Redix.Cluster.command(@cluster, ["SET", key, value])

    def get(key), do: Redix.Cluster.command(@cluster, ["GET", key])

    def del(key), do: Redix.Cluster.command(@cluster, ["DEL", key])

    def pipeline(ops), do: Redix.Cluster.pipeline(@cluster, Enum.map(ops, &to_command/1))

    def transaction(tag, ops) do
      Redix.Cluster.transaction_pipeline(@cluster, Enum.map(ops, &tx_to_command(tag, &1)))
    end

    # op -> Redis command (plain keys)
    def to_command({:set, key, value}), do: ["SET", key, value]
    def to_command({:get, key}), do: ["GET", key]
    def to_command({:del, key}), do: ["DEL", key]

    # tx op -> Redis command, all keys forced into the same slot via a hash tag.
    def tx_to_command(tag, {:set, field, value}), do: ["SET", tx_key(tag, field), value]
    def tx_to_command(tag, {:get, field}), do: ["GET", tx_key(tag, field)]
    def tx_to_command(tag, {:del, field}), do: ["DEL", tx_key(tag, field)]

    def tx_key(tag, field), do: "{#{tag}}.#{field}"
  end

  setup_all do
    case :gen_tcp.connect(~c"localhost", 7000, []) do
      {:ok, socket} -> :gen_tcp.close(socket)
      {:error, _reason} -> flunk("Redis Cluster not available on localhost:7000")
    end

    start_supervised!({Redix.Cluster, nodes: @nodes, name: @cluster, sync_connect: true})

    # Persistent per-node connections reused to flush between property runs.
    flushers =
      for port <- @ports do
        start_supervised!(
          {Redix, host: "localhost", port: port, sync_connect: true, name: :"flush_#{port}"},
          id: :"flush_#{port}"
        )

        :"flush_#{port}"
      end

    %{flushers: flushers}
  end

  property "Redix.Cluster behaves like a key/value map across nodes", [:verbose], %{
    flushers: flushers
  } do
    numtests(
      60,
      forall cmds <- commands(__MODULE__) do
        trap_exit do
          flush_all(flushers)

          {history, state, result} = run_commands(__MODULE__, cmds)

          fail_report = """
          History: #{inspect(history, pretty: true)}

          State: #{inspect(state, pretty: true)}

          Result: #{inspect(result, pretty: true)}
          """

          (result == :ok)
          |> when_fail(IO.puts(fail_report))
          |> aggregate(command_names(cmds))
        end
      end
    )
  end

  ## Generators

  def command(_state) do
    frequency([
      {5, {:call, C, :set, [key(), value()]}},
      {5, {:call, C, :get, [key()]}},
      {2, {:call, C, :del, [key()]}},
      {3, {:call, C, :pipeline, [non_empty(list(pipe_op()))]}},
      {2, {:call, C, :transaction, [tag(), non_empty(list(tx_op()))]}}
    ])
  end

  defp key, do: oneof(@keys)

  defp value, do: utf8(8)

  defp pipe_op do
    oneof([
      {:set, key(), value()},
      {:get, key()},
      {:del, key()}
    ])
  end

  defp tag, do: oneof(["t1", "t2"])

  defp tx_op do
    oneof([
      {:set, tx_field(), value()},
      {:get, tx_field()},
      {:del, tx_field()}
    ])
  end

  defp tx_field, do: oneof(["a", "b", "c"])

  ## Model

  # State is a plain map of full Redis key -> stored value (absent key => nil).
  def initial_state, do: %{}

  def precondition(_state, _call), do: true

  ## Postconditions

  def postcondition(state, {:call, C, :get, [key]}, result) do
    result == {:ok, Map.get(state, key)}
  end

  def postcondition(_state, {:call, C, :set, [_key, _value]}, result) do
    result == {:ok, "OK"}
  end

  def postcondition(state, {:call, C, :del, [key]}, result) do
    result == {:ok, deleted_count(state, [key])}
  end

  def postcondition(state, {:call, C, :pipeline, [ops]}, result) do
    {expected, _final_state} = apply_ops(state, ops, & &1)
    result == {:ok, expected}
  end

  def postcondition(state, {:call, C, :transaction, [tag, ops]}, result) do
    {expected, _final_state} = apply_ops(state, ops, &C.tx_key(tag, &1))
    result == {:ok, expected}
  end

  ## Next state

  def next_state(state, _result, {:call, C, :set, [key, value]}) do
    Map.put(state, key, value)
  end

  def next_state(state, _result, {:call, C, :del, [key]}) do
    Map.delete(state, key)
  end

  def next_state(state, _result, {:call, C, :get, [_key]}) do
    state
  end

  def next_state(state, _result, {:call, C, :pipeline, [ops]}) do
    {_results, final_state} = apply_ops(state, ops, & &1)
    final_state
  end

  def next_state(state, _result, {:call, C, :transaction, [tag, ops]}) do
    {_results, final_state} = apply_ops(state, ops, &C.tx_key(tag, &1))
    final_state
  end

  ## Model helpers

  # Folds a list of ops over the state, returning `{results_in_order, final_state}`.
  # `key_fun` maps an op's raw key field to its full Redis key (identity for plain
  # pipelines, hash-tag wrapping for transactions). Sequential folding matches the
  # cluster's observable behavior: same-key ops land on the same node in order,
  # and distinct keys are independent.
  defp apply_ops(state, ops, key_fun) do
    {reversed_results, final_state} =
      Enum.reduce(ops, {[], state}, fn op, {results, acc} ->
        case op do
          {:set, raw_key, value} ->
            {["OK" | results], Map.put(acc, key_fun.(raw_key), value)}

          {:get, raw_key} ->
            {[Map.get(acc, key_fun.(raw_key)) | results], acc}

          {:del, raw_key} ->
            full_key = key_fun.(raw_key)
            {[deleted_count(acc, [full_key]) | results], Map.delete(acc, full_key)}
        end
      end)

    {Enum.reverse(reversed_results), final_state}
  end

  defp deleted_count(state, keys) do
    Enum.count(keys, &Map.has_key?(state, &1))
  end

  defp flush_all(flushers) do
    for name <- flushers do
      case Redix.command(name, ["FLUSHALL"]) do
        {:ok, _} -> :ok
        {:error, %Redix.Error{message: "READONLY" <> _}} -> :ok
      end
    end

    :ok
  end
end
