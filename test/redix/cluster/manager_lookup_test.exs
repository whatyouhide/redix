defmodule Redix.Cluster.ManagerLookupTest do
  use ExUnit.Case, async: true

  alias Redix.Cluster.Manager

  setup do
    registry = :"lookup_registry_#{System.unique_integer([:positive])}"
    start_supervised!({Registry, keys: :unique, name: registry})
    :ok = Registry.put_meta(registry, :pool_sizes, {3, 3})

    slot_table = :ets.new(:cluster_lookup_slots, [:set, :public])
    %{registry: registry, slot_table: slot_table}
  end

  test "primary lookup prefers connected members and falls back when all are disconnected",
       %{
         registry: registry,
         slot_table: slot_table
       } do
    node_id = "127.0.0.1:7000"
    slot = 1
    pool_size = 3
    preferred_index = 0
    other_index = Enum.find(0..(pool_size - 1), &(&1 != preferred_index))

    preferred = register_member(registry, {node_id, preferred_index}, {:primary, :disconnected})
    other = register_member(registry, {node_id, other_index}, {:primary, :connected})
    :ets.insert(slot_table, {slot, node_id, []})

    assert Manager.get_connection(slot_table, registry, slot) == {:ok, other}

    set_value(other, {:primary, :disconnected})

    assert {:ok, fallback} = Manager.get_connection(slot_table, registry, slot)
    assert fallback in [preferred, other]
  end

  test "node lookup prefers connected members and falls back when all are disconnected",
       %{
         registry: registry
       } do
    node_id = "127.0.0.1:7001"
    address = {"127.0.0.1", 7001}
    preferred_index = 0
    other_index = 1 - preferred_index

    preferred = register_member(registry, {node_id, preferred_index}, {:primary, :disconnected})
    other = register_member(registry, {node_id, other_index}, {:primary, :connected})

    assert Manager.get_connection_by_node(registry, address) == {:ok, other}

    set_value(other, {:primary, :disconnected})

    assert {:ok, fallback} = Manager.get_connection_by_node(registry, address)
    assert fallback in [preferred, other]
  end

  test "replica lookup prefers a connected member across replica pools", %{
    registry: registry,
    slot_table: slot_table
  } do
    slot = 2
    pool_size = 2
    disconnected_node = "127.0.0.1:7002"
    mixed_node = "127.0.0.1:7003"
    preferred_index = 0
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
      assert Manager.get_replica_connection(slot_table, registry, slot) ==
               {:ok, mixed_other}
    end

    set_value(mixed_other, {:replica, :disconnected})

    assert {:ok, fallback} =
             Manager.get_replica_connection(slot_table, registry, slot)

    assert fallback in [mixed_preferred, mixed_other | disconnected_pids]
  end

  test "replica lookup selects the least-busy member across replica nodes", %{
    registry: registry,
    slot_table: slot_table
  } do
    slot = 2
    idle_node = "127.0.0.1:7002"
    busy_node = "127.0.0.1:7003"
    idle_table = :ets.new(:queue, [:ordered_set, :public])
    busy_table = :ets.new(:queue, [:ordered_set, :public])
    :ets.insert(busy_table, [{0, :pending}, {1, :pending}])

    idle = register_member(registry, {idle_node, 0}, {:replica, :connected, idle_table})
    _busy = register_member(registry, {busy_node, 0}, {:replica, :connected, busy_table})
    :ets.insert(slot_table, {slot, "127.0.0.1:7000", [idle_node, busy_node]})

    for _attempt <- 1..20 do
      assert Manager.get_replica_connection(slot_table, registry, slot) == {:ok, idle}
    end
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

  for route <- [:primary, :replica, :redirect, :keyless] do
    test "#{route} lookup spreads ties and selects the smallest queue", ctx do
      node_id = "127.0.0.1:7000"
      :ets.insert(ctx.slot_table, {1, node_id, [node_id]})
      role = if unquote(route) == :replica, do: :replica, else: :primary
      assert lookup(unquote(route), ctx) == :error

      members =
        for index <- 0..2 do
          table = :ets.new(:queue, [:ordered_set, :public])
          pid = register_member(ctx.registry, {node_id, index}, {role, :connected, table})
          {pid, table}
        end

      selected = for _ <- 1..100, do: lookup(unquote(route), ctx)
      assert MapSet.new(selected) == MapSet.new(members, fn {pid, _table} -> {:ok, pid} end)

      [{idle, _table}, {_busy, busy_table}, {_busier, busier_table}] = members
      :ets.insert(busy_table, {0, :pending})
      :ets.insert(busier_table, [{0, :pending}, {1, :pending}])
      assert lookup(unquote(route), ctx) == {:ok, idle}

      # A deleted queue must not look idle.
      :ets.delete(busier_table)
      assert lookup(unquote(route), ctx) == {:ok, idle}
    end
  end

  defp lookup(:keyless, ctx), do: Manager.get_random_connection(ctx.registry)
  defp lookup(:primary, ctx), do: Manager.get_connection(ctx.slot_table, ctx.registry, 1)
  defp lookup(:replica, ctx), do: Manager.get_replica_connection(ctx.slot_table, ctx.registry, 1)

  defp lookup(:redirect, ctx),
    do: Manager.get_connection_by_node(ctx.registry, {"127.0.0.1", 7000})

  defp register_member(registry, key, {role, state}) do
    table = :ets.new(:queue, [:ordered_set, :public])
    register_member(registry, key, {role, state, table})
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
        Registry.update_value(registry, key, fn {_role, _state, table} ->
          {role, state} = value
          {role, state, table}
        end)

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
