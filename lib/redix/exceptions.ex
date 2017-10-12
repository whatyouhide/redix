defmodule Redix.Error do
  @moduledoc """
  Error returned by Redis.

  This exception represents semantic errors returned by Redis: for example,
  non-existing commands or operations on keys with the wrong type (`INCR
  not_an_integer`).
  """

  defexception [:message]

  @type t :: %__MODULE__{message: binary}
end

defmodule Redix.ConnectionError do
  @moduledoc """
  Error in the connection to Redis.

  This exception represents errors in the connection to Redis: for example,
  request timeouts, disconnections, and similar.

  ## Exception fields

  This exception has the following public fields:

    * `:reason` - (atom) the error reason. It can be one of the Redix-specific
      reasons described in the "Error reasons" section below, or any error
      reason returned by functions in the `:gen_tcp` module (see the
      [`:inet.posix/0](http://www.erlang.org/doc/man/inet.html#type-posix) type.

  ## Error reasons

  The `:reason` field can assume a few Redix-specific values:

    * `:closed`: when the connection to Redis is closed (and Redix is
      reconnecting) and the user attempts to talk to Redis

    * `:disconnected`: when the connection drops while a request to Redis is in
      flight.

    * `:timeout`: when Redis doesn't reply to the request in time.

  """

  defexception [:reason]

  def message(%__MODULE__{reason: reason}) do
    format_reason(reason)
  end

  @doc false
  @spec format_reason(term) :: binary
  def format_reason(reason)

  # :inet.format_error/1 doesn't format :tcp_closed.
  def format_reason(:tcp_closed) do
    "TCP connection closed"
  end

  # Manually returned by us when the connection is closed and someone tries to
  # send a command to Redis.
  def format_reason(:closed) do
    "the connection to Redis is closed"
  end

  def format_reason(reason) do
    case :inet.format_error(reason) do
      'unknown POSIX error' ->
        inspect(reason)

      message ->
        List.to_string(message)
    end
  end
end
