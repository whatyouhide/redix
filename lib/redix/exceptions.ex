defmodule Redix.Error do
  defexception [:message]

  @type t :: %__MODULE__{message: binary}
end

defmodule Redix.NetworkError do
  defexception [:message]

  def exception(reason) when is_atom(reason) do
    %__MODULE__{message: inspect(reason)}
  end
end
