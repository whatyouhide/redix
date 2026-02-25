defmodule Redix.Cluster.HashTest do
  use ExUnit.Case, async: true

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
end
