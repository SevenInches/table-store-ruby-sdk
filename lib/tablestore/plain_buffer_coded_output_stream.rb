require 'consts'
require 'tablestore/plain_buffer_crc8'
require 'tablestore/error'

class  PlainBufferCodedOutputStream

  def initialize(output_stream)
    @output_stream = output_stream
  end

  def write_header
    @output_stream.write_raw_little_endian32(HEADER)
  end

  def write_tag(tag)
    @output_stream.write_raw_byte(tag)
  end

  def write_cell_name(name, cell_check_sum)
    write_tag(TAG_CELL_NAME)
    @output_stream.write_raw_little_endian32(name.length)
    @output_stream.write_bytes(name)
    cell_check_sum = PlainBufferCrc8.crc_string(cell_check_sum, name)
    cell_check_sum
  end

  def write_primary_key_value(value, cell_check_sum)
    write_tag(TAG_CELL_VALUE)
    value = value.to_s.split("::").last if [Metadata::INF_MAX, Metadata::INF_MIN, Metadata::PK_AUTO_INCR].include?value
    if value == "INF_MIN"
      @output_stream.write_raw_little_endian32(1)
      @output_stream.write_raw_byte(VT_INF_MIN)
      cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, VT_INF_MIN)
    elsif value == "INF_MAX"
      @output_stream.write_raw_little_endian32(1)
      @output_stream.write_raw_byte(VT_INF_MAX)
      cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, VT_INF_MAX)
    elsif value == "PK_AUTO_INCR"
      @output_stream.write_raw_little_endian32(1)
      @output_stream.write_raw_byte(VT_AUTO_INCREMENT)
      cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, VT_AUTO_INCREMENT)
    elsif value.is_a?(Fixnum)
      @output_stream.write_raw_little_endian32(1 + LITTLE_ENDIAN_64_SIZE)
      @output_stream.write_raw_byte(VT_INTEGER)
      @output_stream.write_raw_little_endian64(value)
      cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, VT_INTEGER)
      cell_check_sum = PlainBufferCrc8.crc_int64(cell_check_sum, value)
    elsif value.is_a?(String)
      prefix_length = LITTLE_ENDIAN_32_SIZE + 1
      @output_stream.write_raw_little_endian32(prefix_length + value.bytes.length)
      @output_stream.write_raw_byte(VT_STRING)
      @output_stream.write_raw_little_endian32(value.length)
      @output_stream.write_bytes(value)
      cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, VT_STRING)
      cell_check_sum = PlainBufferCrc8.crc_int32(cell_check_sum, value.bytes.length)
      cell_check_sum = PlainBufferCrc8.crc_string(cell_check_sum, value)
    # elsif value.is_a?()
    else
      raise TableStoreClientError.new("Unsupported primary key type: #{value.class}")
    end
    cell_check_sum
  end

  def write_column_value_with_checksum(value, cell_check_sum)
    write_tag(TAG_CELL_VALUE)
    if value.is_a?(TrueClass) || value.is_a?(FalseClass)
      @output_stream.write_raw_little_endian32(2)
      @output_stream.write_raw_byte(VT_BOOLEAN)
      @output_stream.write_boolean(value)
      cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, VT_BOOLEAN)
      if value
        cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, 1)
      else
        cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, 0)
      end
    elsif value.is_a?(Fixnum)
      @output_stream.write_raw_little_endian32(1 + LITTLE_ENDIAN_64_SIZE)
      @output_stream.write_raw_byte(VT_INTEGER)
      @output_stream.write_raw_little_endian64(value)
      cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, VT_INTEGER)
      cell_check_sum = PlainBufferCrc8.crc_int64(cell_check_sum, value)
    elsif value.is_a?(String)
      prefix_length = LITTLE_ENDIAN_32_SIZE + 1
      @output_stream.write_raw_little_endian32(prefix_length + value.bytes.length)
      @output_stream.write_raw_byte(VT_STRING)
      @output_stream.write_raw_little_endian32(value.bytes.length)
      @output_stream.write_bytes(value)
      cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, VT_STRING)
      cell_check_sum = PlainBufferCrc8.crc_int32(cell_check_sum, value.bytes.length)
      cell_check_sum = PlainBufferCrc8.crc_string(cell_check_sum, value)
    elsif value.is_a?(Float)
      if SYS_BITS == 64
        double_in_long, = [value].pack("d").unpack("q")
      else
        double_in_long, = [value].pack("d").unpack("l")
      end
      @output_stream.write_raw_little_endian32(1 + LITTLE_ENDIAN_64_SIZE)
      @output_stream.write_raw_byte(VT_DOUBLE)
      @output_stream.write_double(value)
      cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, VT_DOUBLE)
      cell_check_sum = PlainBufferCrc8.crc_int64(cell_check_sum, double_in_long)
    else
      raise TableStoreClientError.new("Unsupported column type: #{value.class}")
    end
    cell_check_sum
  end

  def write_column_value(value)
    if value.is_a?(TrueClass) || value.is_a?(FalseClass)
      @output_stream.write_raw_byte(VT_BOOLEAN)
    elsif value.is_a?(Fixnum)
      @output_stream.write_raw_byte(VT_INTEGER)
      @output_stream.write_raw_little_endian64(value)
    elsif value.is_a?(String)
      @output_stream.write_raw_byte(VT_STRING)
      @output_stream.write_raw_little_endian32(value.bytes.length)
      @output_stream.write_bytes(value)
    elsif value.is_a?(Float)
      @output_stream.write_raw_byte(VT_DOUBLE)
      @output_stream.write_double(value)
    else
      raise TableStoreClientError.new("Unsupported column type: #{value.class}")
    end
  end

  def write_primary_key_column(pk_name, pk_value, row_check_sum)
    cell_check_sum = 0
    write_tag(TAG_CELL)
    cell_check_sum = write_cell_name(pk_name, cell_check_sum)
    cell_check_sum = write_primary_key_value(pk_value, cell_check_sum)
    write_tag(TAG_CELL_CHECKSUM)
    @output_stream.write_raw_byte(cell_check_sum)
    row_check_sum = PlainBufferCrc8.crc_int8(row_check_sum, cell_check_sum)
    row_check_sum
  end

  def write_column(column_name, column_value, timestamp, row_check_sum)
    cell_check_sum = 0
    write_tag(TAG_CELL)
    cell_check_sum = write_cell_name(column_name, cell_check_sum)
    cell_check_sum = write_column_value_with_checksum(column_value, cell_check_sum)

    if timestamp
      write_tag(TAG_CELL_TIMESTAMP)
      @output_stream.write_raw_little_endian64(timestamp)
      cell_check_sum = PlainBufferCrc8.crc_int64(cell_check_sum, timestamp)
    end
    write_tag(TAG_CELL_CHECKSUM)
    @output_stream.write_raw_byte(cell_check_sum)
    PlainBufferCrc8.crc_int8(row_check_sum, cell_check_sum)
  end

  def write_update_column(update_type, column_name, column_value, row_check_sum)
    update_type = update_type.upcase
    cell_check_sum = 0
    write_tag(TAG_CELL)
    cell_check_sum = write_cell_name(column_name, cell_check_sum)
    timestamp = nil
    if column_value
      if column_value.is_a?(Array)
        if column_value[0]
          cell_check_sum = write_column_value_with_checksum(column_value[0], cell_check_sum)
        end
        if column_value[1]
          timestamp = column_value[1]
        end
      else
        cell_check_sum = write_column_value_with_checksum(column_value, cell_check_sum)
      end
    end
    if update_type == "DELETE"
      write_tag(TAG_CELL_TYPE)
      @output_stream.write_raw_byte(DELETE_ONE_VERSION)
      cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, DELETE_ONE_VERSION)
    elsif update_type == "DELETE_ALL"
      write_tag(TAG_CELL_TYPE)
      @output_stream.write_raw_byte(DELETE_ALL_VERSION)
      cell_check_sum = PlainBufferCrc8.crc_int8(cell_check_sum, DELETE_ALL_VERSION)
    end
    if timestamp
      write_tag(TAG_CELL_TIMESTAMP)
      @output_stream.write_raw_little_endian64(timestamp)
      cell_check_sum = PlainBufferCrc8.crc_int64(cell_check_sum, timestamp)
    end
    write_tag(TAG_CELL_CHECKSUM)
    @output_stream.write_raw_byte(cell_check_sum)
    PlainBufferCrc8.crc_int8(row_check_sum, cell_check_sum)
  end

  def write_primary_key(primary_key, row_check_sum)
    write_tag(TAG_ROW_PK)
    primary_key.each do |pk|
      row_check_sum = write_primary_key_column(pk[0], pk[1], row_check_sum)
    end
    row_check_sum
  end

  def write_columns(columns, row_check_sum)
    if columns and columns.length != 0
      write_tag(TAG_ROW_DATA)
      columns.each do |column|
        if column.length == 2
          row_check_sum = write_column(column[0], column[1], nil, row_check_sum)
        elsif column.length == 3
          row_check_sum = write_column(column[0], column[1], column[2], row_check_sum)
        end
      end
      row_check_sum
    end
  end

  def write_update_columns( attribute_columns, row_check_sum)
    if attribute_columns.length != 0
      write_tag(TAG_ROW_DATA)
      attribute_columns.keys.each do |update_type|
        columns = attribute_columns[update_type]
        columns.each do |column|
          if column.is_a?(String)
            row_check_sum = write_update_column(update_type, column, nil, row_check_sum)
          elsif column.length == 2
            row_check_sum = write_update_column(update_type, column[0], [column[1], nil], row_check_sum)
          elsif column.length == 3
            row_check_sum = write_update_column(update_type, column[0], [column[1], column[2]], row_check_sum)
          else
            raise TableStoreClientError.new("Unsupported column format: #{column.to_s}")
          end
        end
      end
    end
    row_check_sum
  end

  def write_delete_marker(row_checksum)
    write_tag(TAG_DELETE_ROW_MARKER)
    PlainBufferCrc8.crc_int8(row_checksum, 1)
  end

  def write_row_checksum(row_checksum)
    write_tag(TAG_ROW_CHECKSUM)
    @output_stream.write_raw_byte(row_checksum)
  end

  def crc_int8(row_checksum, data)
    PlainBufferCrc8.crc_int8(row_checksum, data)
  end
end
