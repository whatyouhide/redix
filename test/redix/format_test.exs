defmodule Redix.FormatTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Redix.Format

  describe "format_host_and_port/2" do
    property "with string host" do
      check all host <- string(:alphanumeric, min_length: 1, max_length: 10),
                port <- integer(0..65535) do
        assert Format.format_host_and_port(host, port) == host <> ":" <> Integer.to_string(port)
      end

      assert Format.format_host_and_port("example.com", 6432) == "example.com:6432"
    end

    property "with Unix path as host" do
      check all path <- string([?a..?z, ?/, ?.], min_length: 1, max_length: 30) do
        assert Format.format_host_and_port({:local, path}, 0) == path
      end
    end
  end
end
