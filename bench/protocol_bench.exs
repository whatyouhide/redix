defmodule ProtocolBench do
  use Benchfella


  @to_serialize ["foo", "bar", "baz", 1, 2, 3, 'foo', 'bar', 'baz', :bong]

  bench "[Red] packing" do
    Red.Protocol.pack(@to_serialize)
  end
  bench "[:redo] packing" do
    :redo_redis_proto.package(@to_serialize)
  end


  @simple_string_longish "+Operation against a key holding the wrong kind of"
    <> "value. Ut eu efficitur nisl. Aliquam."
    <> "value. Ut eu efficitur nisl. Aliquam."
    <> "value. Ut eu efficitur nisl. Aliquam."
    <> "\r\n"

  bench "[Red] parsing - simple string (long-ish)" do
    Red.Protocol.parse(@simple_string_longish)
  end
  bench "[:redo] parsing - simple string (long-ish)" do
    :redo_redis_proto.parse([], {:raw, @simple_string_longish})
  end
  bench "[:eredis] parsing - simple string (long-ish)" do
    :eredis_parser.parse(:eredis_parser.init(), @simple_string_longish)
  end


  @simple_string_short "+OK\r\n"

  bench "[Red] parsing - simple string (short)" do
    Red.Protocol.parse(@simple_string_short)
  end
  bench "[:redo] parsing - simple string (short)" do
    :redo_redis_proto.parse([], {:raw, @simple_string_short})
  end
  bench "[:eredis] parsing - simple string (short)" do
    :eredis_parser.parse(:eredis_parser.init(), @simple_string_short)
  end


  @integer ":1023\r\n"

  bench "[Red] parsing - integer" do
    Red.Protocol.parse(@integer)
  end
  bench "[:redo] parsing - integer" do
    :redo_redis_proto.parse([], {:raw, @integer})
  end
  bench "[:eredis] parsing - integer" do
    :eredis_parser.parse(:eredis_parser.init(), @integer)
  end


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

  bench "[Red] parsing - bulk string" do
    Red.Protocol.parse(@bulk_string)
  end
  bench "[:redo] parsing - bulk string" do
    :redo_redis_proto.parse([], {:raw, @bulk_string})
  end
  bench "[:eredis] parsing - bulk string" do
    :eredis_parser.parse(:eredis_parser.init(), @bulk_string)
  end


  @array_short "*2\r\n$3\r\nfoo\r\n+OK\r\n"

  bench "[Red] parsing - array (short)" do
    Red.Protocol.parse(@array_short)
  end
  bench "[:redo] parsing - array (short)" do
    :redo_redis_proto.parse([], {:raw, @array_short})
  end
  bench "[:eredis] parsing - array (short)" do
    :eredis_parser.parse(:eredis_parser.init(), @array_short)
  end


  @array_long_nelems 50_000
  @array_long "*#{@array_long_nelems}\r\n" <> String.duplicate("$1\r\na\r\n", @array_long_nelems)

  bench "[Red] parsing - array (long)" do
    Red.Protocol.parse(@array_long)
  end
  bench "[:redo] parsing - array (long)" do
    :redo_redis_proto.parse([], {:raw, @array_long})
  end
  bench "[:eredis] parsing - array (long)" do
    :eredis_parser.parse(:eredis_parser.init(), @array_long)
  end
end
