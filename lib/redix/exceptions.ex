defmodule Redix.Error do
  @moduledoc """
  Error returned by Redis.
  """

  defexception [:message]

  @type t :: %__MODULE__{message: binary}
end

defmodule Redix.NetworkError do
  @moduledoc """
  Error in the TCP connection to Redis.
  """

  defexception [:message]

  def exception(reason) when is_atom(reason) do
    %__MODULE__{message: inspect(reason)}
  end
end
