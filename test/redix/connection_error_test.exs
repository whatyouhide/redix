defmodule Redix.ConnectionErrorTest do
  use ExUnit.Case, async: true

  alias Redix.ConnectionError

  test "Exception.message/1 with a POSIX reason" do
    assert Exception.message(%ConnectionError{reason: :eaddrinuse}) == "address already in use"
  end

  test "Exception.message/1 with an unknown reason" do
    assert Exception.message(%ConnectionError{reason: :unknown}) == "unknown POSIX error: unknown"
  end

  test "Exception.message/1 with a TCP/SSL closed message" do
    assert Exception.message(%ConnectionError{reason: :tcp_closed}) == "TCP connection closed"
    assert Exception.message(%ConnectionError{reason: :ssl_closed}) == "SSL connection closed"
  end

  test "Exception.message/1 with a \"wrong role\" tuple" do
    assert Exception.message(%ConnectionError{reason: {:wrong_role, "master"}}) ==
             "wrong role: master"

    assert Exception.message(%ConnectionError{reason: {:wrong_role, "slave"}}) ==
             "wrong role: slave"
  end

  test "Exception.message/1 with the :closed reason" do
    assert Exception.message(%ConnectionError{reason: :closed}) ==
             "the connection to Redis is closed"
  end

  test "Exception.message/1 with the :health_check_timeout reason" do
    assert Exception.message(%ConnectionError{reason: :health_check_timeout}) ==
             "a command went unanswered for longer than the configured :health_check_interval"
  end

  test "Exception.message/1 with a non-atom, non-tuple reason falls back to inspect" do
    assert Exception.message(%ConnectionError{reason: {:some, :other, :tuple}}) ==
             "{:some, :other, :tuple}"
  end
end
