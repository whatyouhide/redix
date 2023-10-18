defmodule Redix.Error do
  @moduledoc """
  Error returned by Redis.

  This exception represents semantic errors returned by Redis: for example,
  non-existing commands or operations on keys with the wrong type (`INCR
  not_an_integer`).
  """

  defexception [:message]

  @typedoc """
  The type for this exception struct.
  """
  @type t() :: %__MODULE__{message: binary()}
end

defmodule Redix.ConnectionError do
  @moduledoc """
  Error in the connection to Redis.

  This exception represents errors in the connection to Redis: for example,
  request timeouts, disconnections, and similar.

  ## Exception fields

  See `t:t/0`.

  ## Error reasons

  The `:reason` field can assume a few Redix-specific values:

    * `:closed`: when the connection to Redis is closed (and Redix is
      reconnecting) and the user attempts to talk to Redis

    * `:disconnected`: when the connection drops while a request to Redis is in
      flight.

    * `:timeout`: when Redis doesn't reply to the request in time.

  """

  @typedoc """
  The type for this exception struct.

  This exception has the following public fields:

    * `:reason` - the error reason. It can be one of the Redix-specific
      reasons described in the "Error reasons" section below, or any error
      reason returned by functions in the `:gen_tcp` module (see the
      [`:inet.posix/0`](http://www.erlang.org/doc/man/inet.html#type-posix) type) or
      `:ssl` module.

  """
  @type t() :: %__MODULE__{reason: atom}

  defexception [:reason]

  @impl true
  def message(%__MODULE__{reason: reason}) do
    format_reason(reason)
  end

  # :inet.format_error/1 doesn't format closed messages.
  defp format_reason(:tcp_closed), do: "TCP connection closed"
  defp format_reason(:ssl_closed), do: "SSL connection closed"

  # Manually returned by us when the connection is closed and someone tries to
  # send a command to Redis.
  defp format_reason(:closed), do: "the connection to Redis is closed"

  if System.otp_release() >= "26" do
    defp format_reason(reason), do: reason |> :inet.format_error() |> List.to_string()
  else
    defp format_reason(reason) do
      case :inet.format_error(reason) do
        ~c"unknown POSIX error" = message when is_atom(reason) -> "#{message}: #{reason}"
        ~c"unknown POSIX error" -> inspect(reason)
        message -> List.to_string(message)
      end
    end
  end
end
