require 'consts'
require 'tablestore/plain_buffer_crc8'
require 'tablestore/error'

class PlainBufferCodedInputStream
  def initialize(input_stream)
    @input_stream = input_stream
  end

  def read_tag
    @input_stream.read_tag
  end

  def check_last_tag_was(tag)
    @input_stream.check_last_tag_was(tag)
  end

  def get_last_tag
    @input_stream.get_last_tag
  end

  def read_header
    @input_stream.read_int32
  end

  def read_primary_key_value(cell_check_sum)
    raise TableStoreClientError.new("Expect TAG_CELL_VALUE but it was " + get_last_tag.to_s) unless check_last_tag_was(TAG_CELL_VALUE)
    @input_stream.read_raw_little_endian32
    column_type = @input_stream.read_raw_byte.ord
    if column_type == VT_INTEGER
      int64_value = @input_stream.read_int64
      cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, VT_INTEGER)
      cell_check_sum = PlainBufferCrc8.crc_int64(cell_check_sum, int64_value)
      read_tag
      return int64_value, cell_check_sum
    elsif column_type == VT_STRING
      value_size = @input_stream.read_int32
      string_value = @input_stream.read_utf_string(value_size)
      cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, VT_STRING)
      cell_check_sum = PlainBufferCrc8.crc_int32(cell_check_sum, value_size)
      cell_check_sum = PlainBufferCrc8.crc_string(cell_check_sum, string_value)
      read_tag
      return string_value, cell_check_sum
    elsif column_type == VT_BLOB
      value_size = @input_stream.read_int32
      binary_value = @input_stream.read_bytes(value_size)
      cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, VT_BLOB)
      cell_check_sum = PlainBufferCrc8.crc_int32(cell_check_sum, value_size)
      cell_check_sum = PlainBufferCrc8.crc_string(cell_check_sum, binary_value)
      read_tag
      return [binary_value], cell_check_sum
    else
      raise TableStoreClientError.new("Unsupported primary key type:" + column_type.to_s)
    end
  end

  def read_column_value(cell_check_sum)
    raise TableStoreClientError.new("Expect TAG_CELL_VALUE but it was " + get_last_tag.to_s) unless check_last_tag_was(TAG_CELL_VALUE)
    @input_stream.read_raw_little_endian32
    column_type = @input_stream.read_raw_byte.ord
    if column_type == VT_INTEGER
      int64_value = @input_stream.read_int64
      cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, VT_INTEGER)
      cell_check_sum = PlainBufferCrc8.crc_int64(cell_check_sum, int64_value)
      read_tag
      return int64_value, cell_check_sum
    elsif column_type == VT_STRING
      value_size = @input_stream.read_int32
      string_value = @input_stream.read_utf_string(value_size)
      cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, VT_STRING)
      cell_check_sum = PlainBufferCrc8.crc_int32(cell_check_sum, value_size)
      cell_check_sum =PlainBufferCrc8.crc_string(cell_check_sum, string_value)
      read_tag
      return string_value, cell_check_sum
    elsif column_type == VT_BLOB
      value_size = @input_stream.read_int32
      binary_value = @input_stream.read_bytes(value_size)
      cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, VT_BLOB)
      cell_check_sum = PlainBufferCrc8.crc_int32(cell_check_sum, value_size)
      cell_check_sum = PlainBufferCrc8.crc_string(cell_check_sum, binary_value)
      read_tag
      return binary_value, cell_check_sum
    elsif column_type == VT_BOOLEAN
      bool_value = @input_stream.read_boolean
      cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, VT_BOOLEAN)
      cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, bool_value ? 1 : 0)
      read_tag
      return bool_value, cell_check_sum
    elsif column_type == VT_DOUBLE
      double_int = @input_stream.read_int64
      cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, VT_DOUBLE)
      cell_check_sum = PlainBufferCrc8.crc_int64(cell_check_sum, double_int)
      read_tag

      if SYS_BITS == 64
          double_value, = [double_int].pack('q').unpack('d')
      else
          double_value, = [double_int].pack('l').unpack('d')
      end
      return double_value, cell_check_sum
    else
      raise TableStoreClientError.new("Unsupported column type: " + column_type.str)
    end
  end

  def read_primary_key_column(row_check_sum)
    raise TableStoreClientError.new("Expect TAG_CELL but it was " + get_last_tag.to_s) unless check_last_tag_was(TAG_CELL)
    read_tag
    raise TableStoreClientError.new("Expect TAG_CELL_NAME but it was " + get_last_tag.to_s) unless check_last_tag_was(TAG_CELL_NAME)
    cell_check_sum = 0
    name_size = @input_stream.read_raw_little_endian32
    column_name = @input_stream.read_utf_string(name_size)
    cell_check_sum = PlainBufferCrc8.crc_string(cell_check_sum, column_name)
    read_tag
    raise TableStoreClientError.new("Expect TAG_CELL_VALUE but it was " + get_last_tag.to_s) unless check_last_tag_was(TAG_CELL_VALUE)
    primary_key_value, cell_check_sum = read_primary_key_value(cell_check_sum)
    if @input_stream.get_last_tag == TAG_CELL_CHECKSUM
      check_sum = @input_stream.read_raw_byte.ord
      raise TableStoreClientError.new("Checksum mismatch. expected:" + check_sum.to_s + ",actual:" + cell_check_sum.to_s) if check_sum != cell_check_sum
      read_tag
    else
      raise TableStoreClientError.new("Expect TAG_CELL_CHECKSUM but it was " + get_last_tag.to_s)
    end
    row_check_sum = PlainBufferCrc8.crc_int8(row_check_sum, cell_check_sum)
    return column_name, primary_key_value, row_check_sum
  end

  def read_column(row_check_sum)
    raise TableStoreClientError.new("Expect TAG_CELL but it was " + get_last_tag.to_s) unless check_last_tag_was(TAG_CELL)
    read_tag
    raise TableStoreClientError.new("Expect TAG_CELL_NAME but it was " + get_last_tag.to_s) unless check_last_tag_was(TAG_CELL_NAME)

    cell_check_sum = 0
    column_value = nil
    timestamp = nil
    name_size = @input_stream.read_raw_little_endian32
    column_name = @input_stream.read_utf_string(name_size)
    cell_check_sum = PlainBufferCrc8.crc_string(cell_check_sum, column_name)
    read_tag
    if get_last_tag == TAG_CELL_VALUE
      column_value, cell_check_sum = read_column_value(cell_check_sum)
    end
    # skip CELL_TYPE
    if get_last_tag == TAG_CELL_TYPE
      cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, cell_type)
      read_tag
    end
    if get_last_tag == TAG_CELL_TIMESTAMP
      timestamp = @input_stream.read_int64
      cell_check_sum = PlainBufferCrc8.crc_int64(cell_check_sum, timestamp)
      read_tag
    end
    if get_last_tag == TAG_CELL_CHECKSUM
      check_sum = @input_stream.read_raw_byte.ord
      raise TableStoreClientError.new("Checksum mismatch. expected:" + check_sum.to_s + ",actual:" + cell_check_sum.to_s) if check_sum != cell_check_sum
      read_tag
    else
      raise TableStoreClientError.new("Expect TAG_CELL_CHECKSUM but it was " + get_last_tag.to_s)
    end
    row_check_sum = PlainBufferCrc8.crc_int8(row_check_sum, cell_check_sum)
    return column_name, column_value, timestamp, row_check_sum
  end

  def read_row_without_header
    row_check_sum = 0
    primary_key = {}
    attributes = {}

    raise TableStoreClientError.new("Expect TAG_ROW_PK but it was " + get_last_tag.to_s) unless check_last_tag_was(TAG_ROW_PK)

    read_tag
    while check_last_tag_was(TAG_CELL)
      name, value, row_check_sum = read_primary_key_column(row_check_sum)
      primary_key[name] = value
    end
    if check_last_tag_was(TAG_ROW_DATA)
      read_tag
      while check_last_tag_was(TAG_CELL)
        column_name, column_value, timestamp, row_check_sum = read_column(row_check_sum)
        attributes[column_name] = column_value
      end
    end
    if check_last_tag_was(TAG_DELETE_ROW_MARKER)
      read_tag
      row_check_sum = PlainBufferCrc8.crc_int8(row_check_sum, 1)
    else
      row_check_sum = PlainBufferCrc8.crc_int8(row_check_sum, 0)
    end

    if check_last_tag_was(TAG_ROW_CHECKSUM)
      check_sum = @input_stream.read_raw_byte.ord
      raise TableStoreClientError.new("Checksum is mismatch.") if check_sum != row_check_sum
      read_tag
    else
      raise TableStoreClientError.new("Expect TAG_ROW_CHECKSUM but it was " + get_last_tag.to_s)
    end
    return primary_key, attributes
  end

  def read_row
    raise TableStoreClientError.new("Invalid header from plain buffer.") if read_header != HEADER
    read_tag
    read_row_without_header
  end

  def read_rows
    raise TableStoreClientError("Invalid header from plain buffer.") if read_header != HEADER
    read_tag
    row_list = []
    while !@input_stream.is_at_end?
      pk, attr = read_row_without_header
      row_list << {"primary_key"=>pk, "attribute_columns"=> attr}
    end
    row_list
  end

end