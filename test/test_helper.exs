ExUnit.start()

defmodule Redix.TestHelpers do
  def silence_log(fun) do
    Logger.remove_backend :console
    fun.()
    Logger.add_backend :console, flush: true
  end
end
