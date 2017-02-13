defmodule Redix.Error do
  @moduledoc """
  Error returned by Redis.
  """

  defexception [:message]

  @type t :: %__MODULE__{message: binary}
end

defmodule Redix.ConnectionError do
  @moduledoc """
  Error in the connection to Redis.
  """

  defexception [:reason]

  def message(%__MODULE__{reason: reason}) do
    format_reason(reason)
  end

  @doc false
  @spec format_reason(term) :: binary
  def format_reason(reason)

  # :inet.format_error/1 doesn't format :tcp_closed or :closed.
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
