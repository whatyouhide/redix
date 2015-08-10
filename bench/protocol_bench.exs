defmodule RespBench do
  use Benchfella

  @simple_string "+Operation against a key holding the wrong kind of"
    <> "value. Ut eu efficitur nisl. Aliquam."
    <> "value. Ut eu efficitur nisl. Aliquam."
    <> "value. Ut eu efficitur nisl. Aliquam."
    <> "\r\n"

  @error "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"

  @integer ":1023\r\n"

  str = """
  $Integer tortor felis, blandit facilisis fermentum sit amet, sollicitudin quis
  lorem. Proin vestibulum nisi tortor, nec elementum enim laoreet nec. Nullam
  sed sodales tellus. Pellentesque pretium lacus et elementum sagittis. Donec
  congue bibendum velit eu elementum. Aenean ultricies est sit amet commodo
  aliquet. Pellentesque non ornare metus. Donec accumsan auctor augue nec
  luctus. Nulla nisl diam, feugiat vel quam eu, pellentesque pharetra
  orci. Quisque congue lobortis ante, vel lobortis tellus bibendum ac. Nunc
  condimentum, urna vel eleifend pretium, felis justo egestas nulla, et euismod
  mi nulla vel ipsum. Suspendisse eleifend maximus aliquet. Morbi vulputate
  augue id velit dignissim vestibulum. Vivamus commodo aliquam odio, vehicula
  gravida justo malesuada in. Fusce interdum nisl nisi, in tincidunt elit
  convallis quis.
  """
  @bulk_string "$#{byte_size(str)}\r\n#{String.strip(str)}\r\n"

  @array "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"

  @to_serialize ~w(Nam suscipit vehicula volutpat Ut Proin sodales blandit elit sed)

  bench "packing (Recs)" do
    Recs.Protocol.pack(@to_serialize)
  end
  bench "packing (:redo)" do
    :redo_redis_proto.package(@to_serialize)
  end

  bench "parsing - simple string (Recs)" do
    Recs.Protocol.parse(@simple_string)
  end
  bench "parsing - simple string (:redo)" do
    :redo_redis_proto.parse([], {:raw, @simple_string})
  end
  bench "parsing - simple string (:eredis)" do
    :eredis_parser.parse(:eredis_parser.init(), @simple_string)
  end

  bench "parsing - integer (Recs)" do
    Recs.Protocol.parse(@integer)
  end
  bench "parsing - integer (:redo)" do
    :redo_redis_proto.parse([], {:raw, @integer})
  end
  bench "parsing - integer (:eredis)" do
    :eredis_parser.parse(:eredis_parser.init(), @integer)
  end

  bench "parsing - bulk string (Recs)" do
    Recs.Protocol.parse(@bulk_string)
  end
  bench "parsing - bulk string (:redo)" do
    :redo_redis_proto.parse([], {:raw, @bulk_string})
  end
  bench "parsing - bulk string (:eredis)" do
    :eredis_parser.parse(:eredis_parser.init(), @bulk_string)
  end

  bench "parsing - array (Recs)" do
    Recs.Protocol.parse(@array)
  end
  bench "parsing - array (:redo)" do
    :redo_redis_proto.parse([], {:raw, @array})
  end
  bench "parsing - array (:eredis)" do
    :eredis_parser.parse(:eredis_parser.init(), @array)
  end
end
