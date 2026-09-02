defmodule Redix.Cluster.ManagerLookupTest do
  use ExUnit.Case, async: true

  alias Redix.Cluster.Manager

  setup do
    registry = :"lookup_registry_#{System.unique_integer([:positive])}"
    start_supervised!({Registry, keys: :unique, name: registry})

    slot_table = :ets.new(:cluster_lookup_slots, [:set, :public])
    %{registry: registry, slot_table: slot_table}
  end

  test "primary lookup skips a disconnected preferred member and falls back to it when all are disconnected",
       %{
         registry: registry,
         slot_table: slot_table
       } do
    node_id = "127.0.0.1:7000"
    slot = 1
    pool_size = 3
    preferred_index = :erlang.phash2(self(), pool_size)
    other_index = Enum.find(0..(pool_size - 1), &(&1 != preferred_index))

    preferred = register_member(registry, {node_id, preferred_index}, {:primary, :disconnected})
    other = register_member(registry, {node_id, other_index}, {:primary, :connected})
    :ets.insert(slot_table, {slot, node_id, []})

    assert Manager.get_connection(slot_table, registry, slot, pool_size) == {:ok, other}

    set_value(other, {:primary, :disconnected})

    assert Manager.get_connection(slot_table, registry, slot, pool_size) == {:ok, preferred}
  end

  test "node lookup skips a disconnected preferred member and falls back to it when all are disconnected",
       %{
         registry: registry
       } do
    node_id = "127.0.0.1:7001"
    address = {"127.0.0.1", 7001}
    pool_size = 2
    preferred_index = :erlang.phash2(self(), pool_size)
    other_index = 1 - preferred_index

    preferred = register_member(registry, {node_id, preferred_index}, {:primary, :disconnected})
    other = register_member(registry, {node_id, other_index}, {:primary, :connected})

    assert Manager.get_connection_by_node(registry, address, self()) == {:ok, other}

    set_value(other, {:primary, :disconnected})

    assert Manager.get_connection_by_node(registry, address, self()) == {:ok, preferred}
  end

  test "replica lookup prefers a connected member across replica pools", %{
    registry: registry,
    slot_table: slot_table
  } do
    slot = 2
    pool_size = 2
    disconnected_node = "127.0.0.1:7002"
    mixed_node = "127.0.0.1:7003"
    preferred_index = :erlang.phash2(self(), pool_size)
    other_index = 1 - preferred_index

    disconnected_pids =
      for index <- 0..(pool_size - 1) do
        register_member(registry, {disconnected_node, index}, {:replica, :disconnected})
      end

    mixed_preferred =
      register_member(registry, {mixed_node, preferred_index}, {:replica, :disconnected})

    mixed_other = register_member(registry, {mixed_node, other_index}, {:replica, :connected})
    :ets.insert(slot_table, {slot, "127.0.0.1:7000", [disconnected_node, mixed_node]})

    for _attempt <- 1..10 do
      assert Manager.get_replica_connection(slot_table, registry, slot, pool_size) ==
               {:ok, mixed_other}
    end

    set_value(mixed_other, {:replica, :disconnected})

    assert {:ok, fallback} =
             Manager.get_replica_connection(slot_table, registry, slot, pool_size)

    assert fallback in [mixed_preferred, mixed_other | disconnected_pids]
  end

  test "random lookup prefers connected primaries, then connected replicas, then any member", %{
    registry: registry
  } do
    disconnected_primary =
      register_member(registry, {"127.0.0.1:7004", 0}, {:primary, :disconnected})

    connected_primary =
      register_member(registry, {"127.0.0.1:7005", 0}, {:primary, :connected})

    replica = register_member(registry, {"127.0.0.1:7006", 0}, {:replica, :connected})

    assert Manager.get_random_connection(registry) == {:ok, connected_primary}

    set_value(connected_primary, {:primary, :disconnected})
    assert Manager.get_random_connection(registry) == {:ok, replica}

    set_value(replica, {:replica, :disconnected})
    assert {:ok, fallback} = Manager.get_random_connection(registry)
    assert fallback in [disconnected_primary, connected_primary, replica]
  end

  defp register_member(registry, key, value) do
    parent = self()
    ref = make_ref()

    pid =
      spawn(fn ->
        {:ok, _owner} = Registry.register(registry, key, value)
        send(parent, {ref, :registered, self()})
        member_loop(registry, key)
      end)

    assert_receive {^ref, :registered, ^pid}
    on_exit(fn -> Process.exit(pid, :kill) end)
    pid
  end

  defp member_loop(registry, key) do
    receive do
      {:set_value, value, caller, ref} ->
        Registry.update_value(registry, key, fn _old_value -> value end)
        send(caller, {ref, :value_set})
        member_loop(registry, key)
    end
  end

  defp set_value(pid, value) do
    ref = make_ref()
    send(pid, {:set_value, value, self(), ref})
    assert_receive {^ref, :value_set}
    :ok
  end
end
