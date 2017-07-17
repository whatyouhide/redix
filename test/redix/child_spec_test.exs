defmodule Redix.ChildSpecTests do
    use ExUnit.Case, async: true

        
    test "child_spec with empty argument" do
        assert Redix.child_spec([]) == %{
            id: Redix,
            start: {Redix, :start_link, []},
            type: :worker
        }
    end

    test "child_spec with only redis_opts" do
        redis_opts = [host: "nonexistent"]
        assert Redix.child_spec([redis_opts])[:start] == {Redix, :start_link, [redis_opts]}
    end

    test "child_spec with only connection_opts" do
        connection_opts = [name: Redix]
        assert Redix.child_spec([nil, connection_opts])[:start] == {Redix, :start_link, [nil, connection_opts]}
        assert Redix.child_spec([[], connection_opts])[:start] == {Redix, :start_link, [[], connection_opts]}
    end

    test "child_spec with redis_opts and connection_opts" do
        redis_opts = [host: "nonexistent"]
        connection_opts = [name: Redix]
        assert Redix.child_spec([redis_opts, connection_opts])[:start] == {Redix, :start_link, [redis_opts, connection_opts]}
    end

    test "child_spec with only name" do
        connection_opts = [name: Redix]
        assert Redix.child_spec(connection_opts)[:start] == {Redix, :start_link, [[], connection_opts]}
    end

    test "child_spec with only name nested in list" do
        connection_opts = [name: Redix]
        assert Redix.child_spec([connection_opts])[:start] == {Redix, :start_link, [[], connection_opts]}
    end

    test "child_spec with uri and name " do
        uri = "redis://localhost:6379"
        connection_opts = [name: Redix]
        assert Redix.child_spec([uri, connection_opts])[:start] == {Redix, :start_link, [uri, connection_opts]}
    end

    test "child_spec with uri only " do
        uri = "redis://localhost:6379"
        assert Redix.child_spec(uri)[:start] == {Redix, :start_link, [uri]}
    end


end
