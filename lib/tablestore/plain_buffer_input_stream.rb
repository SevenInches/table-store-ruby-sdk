require 'tablestore/error'

class PlainBufferInputStream
  def initialize(data_buffer)
    @buffer = data_buffer
    @cur_pos = 0
    @last_tag = 0
  end

  def is_at_end?
    @buffer.length == @cur_pos
  end

  def read_tag
    if is_at_end?
      @last_tag = 0
      return 0
    end
    @last_tag = read_raw_byte
    @last_tag.ord
  end

  def check_last_tag_was(tag)
    @last_tag.ord == tag
  end

  def get_last_tag
    @last_tag.ord
  end

  def read_raw_byte
    raise TableStoreClientError.new("Read raw byte encountered EOF.") if is_at_end?
    pos = @cur_pos
    @cur_pos += 1
    if @buffer[pos].is_a?(Fixnum)
      [@buffer[pos]].chr
    else
      @buffer[pos]
    end
  end

  def read_raw_little_endian64
    read_bytes(8).unpack('q<')[0]
  end

  def read_raw_little_endian32
    read_bytes(4).unpack('i<')[0]
  end

  def read_boolean
    read_bytes(1).unpack('C')[0] == 1
  end

  def read_double
    read_bytes(8).unpack('q<')[0]
  end

  def read_int32
    read_bytes(4).unpack('i<')[0]
  end

  def read_int64
    read_bytes(8).unpack('q<')[0]
  end

  def read_bytes(size)
    raise TableStoreClientError.new("Read bytes encountered EOF.") if @buffer.length - @cur_pos < size
    tmp_pos = @cur_pos
    @cur_pos += size
    @buffer[tmp_pos, size]
  end

  def read_utf_string(size)
    raise TableStoreClientError.new("Read bytes encountered EOF.") if @buffer.length - @cur_pos < size
    utf_str = @buffer[@cur_pos, size]
    @cur_pos += size
    if utf_str.is_a?(String)
      utf_str = utf_str.force_encoding('UTF-8')
    end
    utf_str
  end

  def last_tag
    @last_tag
  end

  def cur_pos
    @cur_pos
  end

  def buffer
    @buffer
  end
end