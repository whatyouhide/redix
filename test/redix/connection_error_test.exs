defmodule Redix.ConnectionErrorTest do
  use ExUnit.Case, async: true

  alias Redix.ConnectionError

  test "Exception.message/1 with a POSIX reason" do
    assert Exception.message(%ConnectionError{reason: :eaddrinuse}) == "address already in use"
  end

  test "Exception.message/1 with an unknown reason" do
    assert Exception.message(%ConnectionError{reason: :unknown}) == ":unknown"
  end
end
