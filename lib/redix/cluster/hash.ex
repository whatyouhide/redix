defmodule Redix.Cluster.Hash do
  @moduledoc false

  import Bitwise

  # Number of hash slots in a Redis Cluster.
  # https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/#key-distribution-model
  @hash_slots 16_384

  # CRC16-XMODEM lookup table, generated at compile time.
  @crc_table (for i <- 0..255 do
                Enum.reduce(0..7, i <<< 8, fn _bit, crc ->
                  crc =
                    if (crc &&& 0x8000) != 0 do
                      bxor(crc <<< 1, 0x1021)
                    else
                      crc <<< 1
                    end

                  band(crc, 0xFFFF)
                end)
              end)
             |> List.to_tuple()

  @doc """
  Computes the Redis Cluster hash slot for the given key.

  If the key contains a hash tag (a substring between `{` and `}`), only the
  hash tag contents are used for hashing. This allows related keys to be
  assigned to the same slot.

  Returns an integer in the range `0..16383`.
  """
  @spec hash_slot(binary()) :: 0..16383
  def hash_slot(key) when is_binary(key) do
    key
    |> extract_hash_tag()
    |> crc16()
    |> rem(@hash_slots)
  end

  @doc """
  Computes the CRC16-XMODEM checksum of the given binary.
  """
  @spec crc16(binary()) :: non_neg_integer()
  def crc16(data) when is_binary(data) do
    crc16(data, 0)
  end

  defp crc16(<<byte, rest::binary>>, crc) do
    index = band(bxor(crc >>> 8, byte), 0xFF)
    new_crc = band(bxor(crc <<< 8, elem(@crc_table, index)), 0xFFFF)
    crc16(rest, new_crc)
  end

  defp crc16(<<>>, crc), do: crc

  @doc """
  Extracts the hash tag from a Redis key.

  A hash tag is the substring between the first occurrence of `{` and the next
  occurrence of `}`. If there is no valid hash tag (no `{`, no `}` after `{`,
  or empty content between `{` and `}`), the entire key is returned.
  """
  @spec extract_hash_tag(binary()) :: binary()
  def extract_hash_tag(key) when is_binary(key) do
    case :binary.match(key, "{") do
      :nomatch ->
        key

      {open_pos, _length = 1} ->
        rest = binary_part(key, open_pos + 1, byte_size(key) - open_pos - 1)

        case :binary.match(rest, "}") do
          :nomatch ->
            key

          {0, 1} ->
            # Empty hash tag like "{}" - use the whole key
            key

          {close_offset, 1} ->
            binary_part(rest, 0, close_offset)
        end
    end
  end
end
