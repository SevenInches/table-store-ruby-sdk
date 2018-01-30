require 'tablestore/error'

class PlainBufferOutputStream
  def initialize(capacity)
    @buffer = []
    @capacity = capacity
  end

  def is_full?
    @capacity <= @buffer.length
  end

  def count
    @buffer.length
  end

  def remain
    @capacity - count
  end

  def clear
    @buffer.clear
  end

  def write_raw_byte(value)
    raise TableStoreClientError.new("The buffer is full") if is_full?
    @buffer << [value].pack("C*")
  end

  def write_raw_little_endian32(value)
    write_bytes([value].pack("i"))
  end

  def write_raw_little_endian64(value)
    write_bytes([value].pack("q"))
  end

  def write_double(value)
    write_bytes([value].pack("d"))
  end

  def write_boolean(value)
    bool_value = value ? 1 : 0
    write_bytes([bool_value].pack("C"))
  end

  def write_bytes(value)
    if @buffer.length + value.length > @capacity
      debugger
      raise TableStoreClientError.new("The buffer is full.")
    end
    bytes = ''
    value.to_s.each_byte do |b|
      bytes += [b].pack("C*")
    end
    @buffer << bytes
  end

  def get_buffer
    @buffer
  end
end
