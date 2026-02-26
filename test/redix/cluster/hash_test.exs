defmodule Redix.Cluster.HashTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Redix.Cluster.Hash

  describe "crc16/1" do
    # Reference values from the Redis Cluster specification.
    test "computes CRC16-XMODEM correctly" do
      assert Hash.crc16("") == 0
      assert Hash.crc16("123456789") == 0x31C3
    end

    test "computes CRC16 for single characters" do
      assert Hash.crc16("a") == 0x7C87
      assert Hash.crc16("A") == 0x58E5
    end
  end

  describe "extract_hash_tag/1" do
    test "returns key as-is when no braces" do
      assert Hash.extract_hash_tag("mykey") == "mykey"
    end

    test "returns key as-is with only opening brace" do
      assert Hash.extract_hash_tag("my{key") == "my{key"
    end

    test "returns key as-is with empty hash tag" do
      assert Hash.extract_hash_tag("my{}key") == "my{}key"
    end

    test "extracts content between first { and next }" do
      assert Hash.extract_hash_tag("{user}.name") == "user"
      assert Hash.extract_hash_tag("prefix{tag}suffix") == "tag"
      assert Hash.extract_hash_tag("{tag}") == "tag"
    end

    test "uses first valid hash tag" do
      assert Hash.extract_hash_tag("{a}{b}") == "a"
      assert Hash.extract_hash_tag("foo{bar}{baz}") == "bar"
    end

    test "handles nested braces" do
      assert Hash.extract_hash_tag("{{tag}}") == "{tag"
    end

    test "returns key when } comes before {" do
      assert Hash.extract_hash_tag("}foo{bar}") == "bar"
    end
  end

  describe "hash_slot/1" do
    test "returns slot in 0..16383 range" do
      slot = Hash.hash_slot("mykey")
      assert slot >= 0 and slot < 16384
    end

    test "same key always maps to same slot" do
      assert Hash.hash_slot("foo") == Hash.hash_slot("foo")
    end

    test "hash tag causes keys to share slots" do
      assert Hash.hash_slot("{user:1000}.name") == Hash.hash_slot("{user:1000}.email")
      assert Hash.hash_slot("{user:1000}.name") == Hash.hash_slot("{user:1000}.age")
    end

    test "different keys without tags may map to different slots" do
      # These specific keys are known to map to different slots
      refute Hash.hash_slot("key1") == Hash.hash_slot("key2")
    end

    # Known slot values from redis-cli "CLUSTER KEYSLOT"
    test "matches redis-cli CLUSTER KEYSLOT values" do
      assert Hash.hash_slot("foo") == 12182
      assert Hash.hash_slot("bar") == 5061
      assert Hash.hash_slot("hello") == 866
      assert Hash.hash_slot("{user:1}") == Hash.hash_slot("user:1")
    end
  end

  describe "crc16/1 properties" do
    property "always returns a 16-bit unsigned integer" do
      check all data <- binary(min_length: 0, max_length: 200) do
        assert Hash.crc16(data) in 0..0xFFFF
      end
    end

    property "is deterministic" do
      check all data <- binary(min_length: 0, max_length: 200) do
        assert Hash.crc16(data) == Hash.crc16(data)
      end
    end
  end

  describe "extract_hash_tag/1 properties" do
    property "returns a binary for any binary input" do
      check all key <- binary(min_length: 0, max_length: 100) do
        assert is_binary(Hash.extract_hash_tag(key))
      end
    end

    property "result is always a substring of the key" do
      check all key <- string(:printable, min_length: 1, max_length: 100) do
        tag = Hash.extract_hash_tag(key)
        assert String.contains?(key, tag)
      end
    end

    property "keys without braces are returned as-is" do
      check all key <- string([?a..?z, ?0..?9, ?_, ?:, ?.], min_length: 1, max_length: 50) do
        assert Hash.extract_hash_tag(key) == key
      end
    end

    property "keys with empty {} are returned as-is" do
      check all prefix <- string(:alphanumeric, min_length: 0, max_length: 20),
                suffix <- string(:alphanumeric, min_length: 0, max_length: 20) do
        key = prefix <> "{}" <> suffix
        assert Hash.extract_hash_tag(key) == key
      end
    end

    property "keys with {tag} return the tag" do
      check all prefix <- string(:alphanumeric, min_length: 0, max_length: 20),
                tag <- string(:alphanumeric, min_length: 1, max_length: 20),
                suffix <- string(:alphanumeric, min_length: 0, max_length: 20) do
        key = prefix <> "{" <> tag <> "}" <> suffix
        assert Hash.extract_hash_tag(key) == tag
      end
    end
  end

  describe "hash_slot/1 properties" do
    property "always returns a slot in 0..16383" do
      check all key <- binary(min_length: 1, max_length: 200) do
        slot = Hash.hash_slot(key)
        assert slot >= 0 and slot < 16384
      end
    end

    property "is deterministic" do
      check all key <- binary(min_length: 1, max_length: 200) do
        assert Hash.hash_slot(key) == Hash.hash_slot(key)
      end
    end

    property "keys sharing a hash tag always map to the same slot" do
      check all tag <- string(:alphanumeric, min_length: 1, max_length: 20),
                suffix1 <- string(:alphanumeric, min_length: 0, max_length: 20),
                suffix2 <- string(:alphanumeric, min_length: 0, max_length: 20) do
        key1 = "{" <> tag <> "}." <> suffix1
        key2 = "{" <> tag <> "}." <> suffix2
        assert Hash.hash_slot(key1) == Hash.hash_slot(key2)
      end
    end

    property "{tag} hashes the same as the bare tag" do
      check all tag <- string(:alphanumeric, min_length: 1, max_length: 50) do
        assert Hash.hash_slot("{" <> tag <> "}") == Hash.hash_slot(tag)
      end
    end
  end
end
