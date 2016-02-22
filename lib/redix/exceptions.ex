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

  def exception(reason) when is_atom(reason) do
    %__MODULE__{message: format_reason(reason)}
  end

  defp format_reason(:empty_command), do: "an empty command ([]) is not a valid Redis command"
  defp format_reason(other), do: Utils.format_error(other)
end
