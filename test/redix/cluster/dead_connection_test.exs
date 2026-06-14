defmodule Redix.Cluster.DeadConnectionTest do
  use ExUnit.Case, async: true

  alias Redix.Cluster.FakeNode
  alias Redix.Cluster.Hash

  # Regression tests for issue #317: a node connection that dies while a command is
  # being routed to it must surface a connection-error *value*, not exit the calling
  # process. In cluster mode the connection pid is internal and its death is routine
  # (`ensure_connections` terminates the connection of every node that leaves the
  # cluster), so a command racing a scale-in/reshard would otherwise crash the caller
  # via Redix.pipeline/3's `{:redix_exited_during_call, _}` exit.
  #
  # We reproduce the race deterministically with a stand-in "connection" process that
  # registers itself under a node id (so the routing code resolves it like any live
  # connection) and then exits the moment a pipeline is cast to it — exactly the
  # window the issue describes (lookup returns a live pid, it dies, the pipeline runs
  # against it). No Docker cluster needed; we wire up the registry and slot table by
  # hand, following the fake-node pattern from redirection_test.exs.

  setup do
    cluster = :"deadconn_#{System.unique_integer([:positive])}"

    start_supervised!({Registry, keys: :unique, name: :"#{cluster}_registry"})
    start_supervised!({Task.Supervisor, name: :"#{cluster}_task_supervisor"})

    :ets.new(:"#{cluster}_slots", [:named_table, :public, :set])
    :ets.insert(:"#{cluster}_slots", {:discovery_attempted, true})

    %{cluster: cluster}
  end

  test "a single-node command whose connection dies returns a connection error, not a caller exit",
       %{cluster: cluster} do
    node_id = "127.0.0.1:65000"
    dying_connection(cluster, node_id)
    route_slot(cluster, Hash.hash_slot("x"), node_id)

    # Before the fix this exited the caller with {:redix_exited_during_call, :shutdown};
    # the test process reaching the assertion at all proves it survived.
    assert Redix.Cluster.command(cluster, ["GET", "x"]) ==
             {:error, %Redix.ConnectionError{reason: :closed}}
  end

  test "a dying node in a multi-node pipeline fills its slots with an error and keeps the " <>
         "healthy node's results",
       %{cluster: cluster} do
    # One healthy node serving its slot...
    good = FakeNode.start_connected(cluster, fn ["GET", _] -> "$2\r\nhi\r\n" end)
    good_slot = Hash.hash_slot("good")
    route_slot(cluster, good_slot, good)

    # ...and one node whose connection dies the moment we pipeline to it.
    bad_id = "127.0.0.1:65001"
    dying_connection(cluster, bad_id)
    bad_slot = Hash.hash_slot("bad")
    route_slot(cluster, bad_slot, bad_id)

    # Sanity: the keys land in different slots, so the pipeline really splits across
    # two nodes and runs each group as a Task (exercising the async_nolink path).
    assert good_slot != bad_slot

    # The dead node's command is filled with a connection-error value at its original
    # index; the healthy node's result stays visible. The crashing group's Task no
    # longer takes the caller down with it.
    assert Redix.Cluster.pipeline(cluster, [["GET", "good"], ["GET", "bad"]]) ==
             {:ok, ["hi", %Redix.ConnectionError{reason: :closed}]}
  end

  test "a transaction whose connection dies returns a connection error, not a caller exit", %{
    cluster: cluster
  } do
    node_id = "127.0.0.1:65002"
    dying_connection(cluster, node_id)
    route_slot(cluster, Hash.hash_slot("x"), node_id)

    assert Redix.Cluster.transaction_pipeline(cluster, [["SET", "x", "1"]]) ==
             {:error, %Redix.ConnectionError{reason: :closed}}
  end

  ## Helpers

  defp route_slot(cluster, slot, node) do
    :ets.insert(:"#{cluster}_slots", {slot, "#{node}", _replicas = []})
  end

  # Spawns a process that registers itself under `node_id` in the cluster registry
  # (so the routing code resolves it as the slot's connection) and then exits as soon
  # as it receives the pipeline cast — standing in for a real Redix connection pid
  # terminated by the Manager between resolution and use.
  defp dying_connection(cluster, node_id) do
    test = self()

    pid =
      spawn(fn ->
        {:ok, _} = Registry.register(:"#{cluster}_registry", node_id, _value = :primary)
        send(test, {:registered, self()})

        receive do
          _cast -> exit(:shutdown)
        end
      end)

    assert_receive {:registered, ^pid}
    pid
  end
end
