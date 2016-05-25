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

  alias Redix.Utils

  defexception [:message]

  def exception(reason) when is_binary(reason) do
    %__MODULE__{message: reason}
  end

  def exception(reason) do
    %__MODULE__{message: format_reason(reason)}
  end

  defp format_reason(:empty_command),
    do: "an empty command ([]) is not a valid Redis command"
  defp format_reason({:pubsub_command, command}) when is_binary(command),
    do: "Pub/Sub commands (#{command} in this case) are not supported by Redix." <>
        "Use the Redix.PubSub project (https://github.com/whatyouhide/redix_pubsub) " <>
        "for Pub/Sub functionality support"
  defp format_reason(other),
    do: Utils.format_error(other)
end
