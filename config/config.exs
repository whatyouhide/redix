use Mix.Config

config :redix,
  redis_host: 'localhost',
  redis_port: 6379

import_config "#{Mix.env}.exs"
