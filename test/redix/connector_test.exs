defmodule Redix.ConnectorTest do
  use ExUnit.Case, async: true

  alias Redix.Connector

  describe "build_socket_opts/2 with :ssl" do
    test "enables peer verification and browser-style hostname checking by default" do
      opts = Connector.build_socket_opts(:ssl, [])

      assert opts[:verify] == :verify_peer
      assert opts[:depth] == 3

      assert [match_fun: match_fun] = opts[:customize_hostname_check]
      assert is_function(match_fun, 2)
      assert match_fun == :public_key.pkix_verify_hostname_match_fun(:https)
    end

    test "lets the user override individual defaults without dropping the others" do
      opts = Connector.build_socket_opts(:ssl, verify: :verify_none)

      # Overridden by the user.
      assert opts[:verify] == :verify_none

      # Still filled in by the defaults.
      assert opts[:depth] == 3
      assert [match_fun: _] = opts[:customize_hostname_check]
    end

    test "lets the user override customize_hostname_check itself" do
      custom = [match_fun: fn _ref_id, _present_id -> true end]
      opts = Connector.build_socket_opts(:ssl, customize_hostname_check: custom)

      assert opts[:customize_hostname_check] == custom
      # The user value is not duplicated by the default.
      assert keyword_get_values(opts, :customize_hostname_check) == [custom]
    end

    test "user options take precedence and are not duplicated by defaults" do
      opts = Connector.build_socket_opts(:ssl, verify: :verify_none, depth: 1)

      assert keyword_get_values(opts, :verify) == [:verify_none]
      assert keyword_get_values(opts, :depth) == [1]
    end
  end

  # The returned options list starts with the bare atom `:binary`, so it isn't a
  # proper keyword list and Keyword.* functions would raise on it. Filter to pairs.
  defp keyword_get_values(opts, key) do
    opts
    |> Enum.filter(&match?({_, _}, &1))
    |> Keyword.get_values(key)
  end

  describe "build_socket_opts/2 with :gen_tcp" do
    test "does not inject SSL defaults" do
      opts = Connector.build_socket_opts(:gen_tcp, [])

      refute Keyword.has_key?(opts, :verify)
      refute Keyword.has_key?(opts, :customize_hostname_check)
    end
  end
end
