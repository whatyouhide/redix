defmodule Redix.TestPorts do
  @moduledoc false

  # Centralizes the host-side ports exposed by docker-compose so tests can be
  # pointed at non-default ports via REDIX_*_PORT env vars. Defaults match the
  # original docker-compose.yml.

  @ports %{
    base: {"REDIX_BASE_PORT", "6379"},
    pubsub: {"REDIX_PUBSUB_PORT", "6380"},
    auth: {"REDIX_AUTH_PORT", "16379"},
    acl: {"REDIX_ACL_PORT", "6385"},
    stunnel: {"REDIX_STUNNEL_PORT", "6384"},
    disallowed_client: {"REDIX_DISALLOWED_CLIENT_PORT", "6386"}
  }

  @spec port(atom()) :: :inet.port_number()
  def port(name) when is_map_key(@ports, name) do
    {env_var, default} = Map.fetch!(@ports, name)
    env_var
    |> System.get_env(default)
    |> String.to_integer()
  end
end
