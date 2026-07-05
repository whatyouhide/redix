defmodule Redix.Cluster.KeyResolver do
  @moduledoc false

  # Resolves the hash slot of commands that fall outside `Redix.Cluster.CommandParser`'s
  # static table (CommandParser returns `:unknown` for those). Rather than blindly routing
  # such a command to a random node, we ask the server how it routes it — via `COMMAND
  # INFO`, falling back to `COMMAND GETKEYS` for movable-key commands — and cache the
  # per-command-name answer so later calls resolve locally with no round-trip.

  alias Redix.Cluster.{Hash, Manager}

  # Upper bound on the per-cluster command cache (issue #329). Real Redis exposes a
  # few hundred commands, so genuine traffic never approaches this; the cap only
  # stops the cache from growing without bound on a pathological stream of distinct
  # unknown command names. Past the cap, uncached commands keep resolving per-call
  # (to a random node), exactly as they did before any caching existed.
  @command_cache_max_size 2048

  @doc """
  Replaces every `:unknown` slot in `indexed_commands` with a resolved slot.

  `indexed_commands` is a list of `{index, command, slot}`, where `slot` is an integer,
  `:no_slot`, or `:unknown`. Each `:unknown` entry is replaced with an integer slot (when
  a key is found) or `:no_slot` (no key, or the lookup itself failed — a genuinely unknown
  command, or no reachable node); on `:no_slot` the command keeps the prior behavior of
  being sent to a random node. Already-resolved entries pass through untouched.
  """
  @spec resolve_unknown_slots([{non_neg_integer(), Redix.command(), term()}], atom(), atom()) ::
          [{non_neg_integer(), Redix.command(), term()}]
  def resolve_unknown_slots(indexed_commands, registry, command_cache) do
    unknowns = for {idx, cmd, :unknown} <- indexed_commands, do: {idx, cmd}

    if unknowns == [] do
      indexed_commands
    else
      slots_by_index = resolve_unknowns(unknowns, registry, command_cache)

      Enum.map(indexed_commands, fn
        {idx, cmd, :unknown} -> {idx, cmd, Map.fetch!(slots_by_index, idx)}
        {_idx, _cmd, _slot} = resolved -> resolved
      end)
    end
  end

  # Resolves a list of `{idx, command}` unknowns to a `%{idx => slot | :no_slot}` map.
  #
  # The key specification of a command (first-key position, or "movable" keys) is stable,
  # so we learn it once via COMMAND INFO and cache it per command name. Subsequent calls
  # to the same command resolve locally with no network round-trip. Commands Redis reports
  # as having *movable* keys (e.g. MIGRATE, BLMPOP) can't be pinned to a fixed position, so
  # those fall back to a per-call COMMAND GETKEYS — but they're rare and still cached as
  # "movable" so we don't re-issue COMMAND INFO for them.
  defp resolve_unknowns(unknowns, registry, command_cache) do
    case Manager.get_random_connection(registry) do
      {:ok, conn} ->
        cache_missing_keyspecs(conn, command_cache, unknowns)
        slots_from_keyspecs(conn, command_cache, unknowns)

      :error ->
        Map.new(unknowns, fn {idx, _cmd} -> {idx, :no_slot} end)
    end
  end

  # Fetches and caches the key specification for any command name not already cached.
  #
  # Guarded as a whole against the command-cache table being momentarily gone
  # (torn down mid-:one_for_all-restart of the cluster tree): this is a
  # best-effort cache population with no return value callers depend on, so a
  # table-missing race simply skips caching for this call, same as the existing
  # "malformed reply" no-op below. COMMAND INFO is retried next time the table
  # (and thus the cluster) is back.
  defp cache_missing_keyspecs(conn, command_cache, unknowns) do
    Manager.guard_missing_table(
      fn ->
        missing =
          unknowns
          |> Enum.map(fn {_idx, cmd} -> command_name(cmd) end)
          |> Enum.uniq()
          |> Enum.reject(&(:ets.lookup(command_cache, &1) != []))

        if missing != [] and :ets.info(command_cache, :size) < @command_cache_max_size do
          # safe_command, not Redix.command: a randomly-chosen `conn` that died mid-flight
          # would otherwise exit the *caller* (A1). The caught exit yields an `{:error, _}`
          # tuple, which falls through to the `_other` clause (nothing cached, commands
          # resolve to :no_slot for this call, COMMAND INFO retried next time).
          case safe_command(conn, ["COMMAND", "INFO" | missing]) do
            {:ok, infos} when length(infos) == length(missing) ->
              missing
              |> Enum.zip(infos)
              |> Enum.each(fn {name, info} ->
                :ets.insert(command_cache, {name, keyspec_from_info(info)})
              end)

            # A malformed *whole* reply (wrong length) or a connection error isn't cached:
            # the commands resolve to :no_slot for this call and we retry COMMAND INFO next
            # time. Note this is only about the whole reply — a well-formed reply whose entry
            # for a given command is `nil` (a command the server doesn't know) *is* cached as
            # :no_key by keyspec_from_info/1.
            _other ->
              :ok
          end
        end
      end,
      :ok
    )
  end

  # COMMAND INFO reply per command: [name, arity, flags, first_key, last_key, step | _].
  # We route on the first key, so we only need first_key (1-based, command name at 0) and
  # whether the keys are movable.
  defp keyspec_from_info([_name, _arity, flags, first_key, _last_key, _step | _]) do
    cond do
      "movablekeys" in flags -> :movable
      first_key >= 1 -> {:first_key, first_key}
      true -> :no_key
    end
  end

  # A command the server doesn't know comes back as `nil` (and any other unparseable
  # entry lands here too). We cache it as :no_key deliberately: the answer is stable, so
  # re-issuing COMMAND INFO for it on every call would be wasted work. This per-entry
  # caching is distinct from the malformed/short *whole* reply that cache_missing_keyspecs/3
  # leaves uncached.
  defp keyspec_from_info(_other), do: :no_key

  defp slots_from_keyspecs(conn, command_cache, unknowns) do
    # Static-position and keyless commands resolve locally; movable ones need a per-call
    # COMMAND GETKEYS, which we batch into a single pipeline.
    {movable, local} =
      Enum.split_with(unknowns, fn {_idx, cmd} ->
        cached_keyspec(command_cache, cmd) == :movable
      end)

    local_slots =
      Map.new(local, fn {idx, cmd} ->
        {idx, slot_from_keyspec(cached_keyspec(command_cache, cmd), cmd)}
      end)

    Map.merge(local_slots, getkeys_slots(conn, movable))
  end

  defp cached_keyspec(command_cache, cmd) do
    Manager.guard_missing_table(
      fn ->
        case :ets.lookup(command_cache, command_name(cmd)) do
          [{_name, keyspec}] -> keyspec
          [] -> :no_key
        end
      end,
      :no_key
    )
  end

  defp slot_from_keyspec({:first_key, position}, cmd) do
    # first_key counts the command name as position 0, so it indexes straight into cmd.
    case Enum.at(cmd, position) do
      nil -> :no_slot
      key -> Hash.hash_slot(to_string(key))
    end
  end

  defp slot_from_keyspec(_no_key_or_unknown, _cmd), do: :no_slot

  # Per-call COMMAND GETKEYS for movable-key commands, batched into one pipeline.
  defp getkeys_slots(_conn, []), do: %{}

  defp getkeys_slots(conn, movable) do
    getkeys = Enum.map(movable, fn {_idx, cmd} -> ["COMMAND", "GETKEYS" | cmd] end)

    # safe_pipeline, not Redix.pipeline: `conn` is a randomly-chosen node that may
    # already be dying (topology churn), and an uncaught exit here would crash the
    # *caller* instead of degrading to :no_slot (A1). The caught exit surfaces as
    # `{:error, _}`, which the clause below maps to :no_slot.
    case safe_pipeline(conn, getkeys) do
      {:ok, results} ->
        movable
        |> Enum.zip(results)
        |> Map.new(fn {{idx, _cmd}, result} -> {idx, slot_from_getkeys(result)} end)

      {:error, _reason} ->
        Map.new(movable, fn {idx, _cmd} -> {idx, :no_slot} end)
    end
  end

  # COMMAND GETKEYS returns the list of keys for a command. We route on the first one. A
  # command with no keys comes back as a Redix.Error, which falls through to :no_slot.
  defp slot_from_getkeys([key | _]) when is_binary(key), do: Hash.hash_slot(key)
  defp slot_from_getkeys(_other), do: :no_slot

  defp command_name([name | _]), do: name |> to_string() |> String.upcase()
  defp command_name([]), do: ""

  # Redix.pipeline/command *exit* the caller with {:redix_exited_during_call, reason} when
  # the connection pid is already dead. The conn used here is an internal, randomly-chosen
  # node that can die during topology churn, so catch the exit and turn it into a
  # connection-error value (issue #317, A1) — the callers above degrade it to :no_slot.
  defp safe_command(conn, command) do
    Redix.command(conn, command)
  catch
    :exit, {:redix_exited_during_call, _reason} ->
      {:error, %Redix.ConnectionError{reason: :closed}}
  end

  defp safe_pipeline(conn, commands) do
    Redix.pipeline(conn, commands)
  catch
    :exit, {:redix_exited_during_call, _reason} ->
      {:error, %Redix.ConnectionError{reason: :closed}}
  end
end
