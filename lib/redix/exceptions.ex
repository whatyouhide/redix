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

  defexception [:message]

  def exception(reason) when is_binary(reason),
    do: %__MODULE__{message: reason}
  def exception(reason),
    do: %__MODULE__{message: Redix.format_error(reason)}
end
