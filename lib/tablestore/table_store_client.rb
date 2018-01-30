require 'os'
require 'protobuf/table_store.pb'
require 'protobuf/table_store_filiter.pb'
require 'consts'
require 'tablestore/plain_buffer_coded_output_stream'
require 'tablestore/plain_buffer_output_stream'
require 'tablestore/plain_buffer_coded_input_stream'
require 'tablestore/plain_buffer_input_stream'

class TableStoreClient

  ##Encode
  def encode_create_table(table_meta, table_options, reserved_throughput)
    proto = CreateTableRequest.new

    meta_proto = TableMeta.new
    meta_proto.table_name = table_meta.table_name
    meta_proto.table_name = table_meta.table_name
    table_meta.schema_of_primary_key.each do |primary_key|
      meta_proto.primary_key << make_schemas_with_list(primary_key)
    end
    proto.table_meta = meta_proto

    proto.table_options = make_table_options(table_options)

    rt_proto = ReservedThroughput.new
    rt_proto.capacity_unit = make_capacity_unit(reserved_throughput.capacity_unit)
    proto.reserved_throughput = rt_proto
    proto.serialize_to_string
  end

  def encode_list_table
    proto = ListTableRequest.new
    proto.serialize_to_string
  end

  def encode_update_table(table_name, table_options, reserved_throughput)
    proto = UpdateTableRequest.new
    proto.table_name = table_name
    if reserved_throughput
      rt_proto = ReservedThroughput.new
      rt_proto.capacity_unit = make_capacity_unit(reserved_throughput.capacity_unit)
      proto.reserved_throughput = rt_proto
    end
    if table_options
      proto.table_options = make_table_options(table_options)
    end
    proto.serialize_to_string
  end

  def encode_delete_table(table_name)
    proto = DeleteTableRequest.new
    proto.table_name = table_name
    proto.serialize_to_string
  end

  def encode_get_range_request(request)
    proto = GetRangeRequest.new
    proto.table_name = request[:table_name]
    proto.direction = request[:direction]
    proto.inclusive_start_primary_key = serialize_primary_key(request[:inclusive_start_primary_key])
    proto.exclusive_end_primary_key = serialize_primary_key(request[:exclusive_end_primary_key])
    proto.max_versions = request[:max_version]
    proto.limit = request[:limit]
    if request[:column_filter]
      proto.filter = make_column_condition(request[:column_filter]).serialize_to_string
    end
    proto.serialize_to_string
  end

  def encode_put_row(table_name, row, condition)
    proto = PutRowRequest.new
    proto.table_name = table_name
    condition = Condition.new(RowExistenceExpectation::IGNORE) if condition.nil?
    contion_proto = Condition.new
    proto.condition = make_condition(contion_proto, condition)
    proto.row = serialize_for_put_row(row.primary_key, row.attribute_columns)
    proto.serialize_to_string
  end

  def encode_get_row(table_name, primary_key, columns_to_get, column_filter, max_version)
    proto = GetRowRequest.new
    proto.table_name = table_name
    make_repeated_column_names(proto.columns_to_get, columns_to_get)
    if column_filter
      proto.filter = make_column_condition(column_filter).serialize_to_string
    end
    proto.primary_key = serialize_primary_key(primary_key)
    proto.max_versions = max_version if max_version
    proto.serialize_to_string
  end

  def encode_update_row(teble_name, row, condition)
    proto = UpdateRowRequest.new
    proto.table_name = teble_name
    condition = Condition.new(RowExistenceExpectation::IGNORE) if condition.nil?
    condition_proto = Condition.new
    proto.condition = make_condition(condition_proto, condition)
    if return_type == ReturnType::RT_PK
      return_content = ReturnContent.new
      return_content.return_type = RT_PK
      proto.return_content = return_content
    end
    proto.row_change = serialize_for_update_row(row.primary_key, row.attribute_columns)
    proto.serialize_to_string
  end

  def encode_delete_row(table_name, row, condition)
    proto = DeleteRowRequest.new
    proto.table_name = table_name
    condition = Condition.new(RowExistenceExpectation::IGNORE) if condition.nil?
    condition_proto = Condition.new
    proto.condition = make_condition(condition_proto, condition)
    proto.primary_key = serialize_for_delete_row(row.primary_key)
    proto.serialize_to_string
  end

  ##Decode
  def decode_list_table(body)
    proto = ListTableResponse.new
    proto.parse_from_string(body)
    names = proto.table_names
    names
  end

  def decode_get_range_request(body)
    proto = GetRangeResponse.new
    proto.parse_from_string(body)

    next_start_pk = nil
    row_list = []

    if proto.next_start_primary_key.length != 0
      inputStream = PlainBufferInputStream.new(proto.next_start_primary_key)
      codedInputStream = PlainBufferCodedInputStream.new(inputStream)
      next_start_pk, att = codedInputStream.read_row
    end

    if proto.rows.length != 0
      inputStream = PlainBufferInputStream.new(proto.rows)
      codedInputStream = PlainBufferCodedInputStream.new(inputStream)
      row_list = codedInputStream.read_rows
    end

    return next_start_pk, row_list
  end

  def decode_get_row(body)
    proto = GetRowResponse.new
    proto.parse_from_string(body)

    return_row = nil
    if proto.row.length > 0
      inputStream = PlainBufferInputStream.new(proto.row)
      codedInputStream = PlainBufferCodedInputStream.new(inputStream)
      return_row = codedInputStream.read_row
    end
    return_row
  end

  def decode_put_row(body)
    proto = PutRowResponse.new
    proto.parse_from_string(body)
    return_row = nil
    if proto.row.length != 0
      inputStream = PlainBufferInputStream.new(proto.row)
      codedInputStream = PlainBufferCodedInputStream.new(inputStream)
      return_row = codedInputStream.read_row
    end
    return_row
  end

  def decode_batch_get_row(body)
    proto = BatchGetRowResponse.new
    proto.parse_from_string(body)
    rows = []
    proto.tables.each do |table_item|
      rows << parse_get_row_item(table_item.rows)
    end
    rows
  end

  ##Make
  def make_batch_get_row(request)
    proto = BatchGetRowRequest.new
    request.items.each do |item|
      table_value = item[1]
      table_item = TableInBatchGetRowRequest.new
      table_item.table_name = table_value.table_name
      make_repeated_column_names(table_item.columns_to_get, table_value.columns_to_get)

      if table_value.column_filter
        table_item.filter = make_column_condition(table_value.column_filter).serialize_to_string
      end

      table_value.primary_keys.each do |pk|
        table_item.primary_key << serialize_primary_key(pk)
      end

      if table_value.max_version
          table_item.max_versions = table_value.max_version
      end
      if table_value.time_range
        if table_value.time_range.is_a?(Array)
          table_item.time_range.start_time = table_value.time_range[0]
          table_item.time_range.end_time = table_value.time_range[1]
        elsif table_value.is_a?(Fixnum)
          table_item.time_range.specific_time = table_value.time_range
        end
      end
      if table_value.start_column
        table_item.start_column = table_value.start_column
      end
      if table_value.end_column
          table_item.end_column = table_value.end_column
      end
      proto.tables << table_item
    end
    proto.serialize_to_string
  end

  def make_batch_write_row(request)
    proto = BatchWriteRowRequest.new
    request.items.each do |item|
      table_value = item[1]
      table_item = TableInBatchWriteRowRequest.new
      table_item.table_name = table_value.table_name

      table_value.row_items.each do |row_item|
        if row_item.type == Metadata::BatchWriteRowType::PUT
          row = RowInBatchWriteRowRequest.new
          table_item.rows << make_put_row_item(row, row_item)
        end
        if row_item.type == Metadata::BatchWriteRowType::UPDATE
          row = RowInBatchWriteRowRequest.new
          table_item.rows << make_update_row_item(row, row_item)
        end
        if row_item.type == Metadata::BatchWriteRowType::DELETE
          row = RowInBatchWriteRowRequest.new
          table_item.rows << make_delete_row_item(row, row_item)
        end
      end
      proto.tables << table_item
    end
    proto.serialize_to_string
  end

  def make_put_row_item(proto, put_row_item)
    condition = put_row_item.condition
    if condition.nil?
      condition = Condition.new(Metadata::RowExistenceExpectation::IGNORE)
    end
    condition_proto = Condition.new
    proto.condition = make_condition(condition_proto, condition)
    if put_row_item.return_type == ReturnType::RT_PK
      return_content = ReturnContent.new
      return_content.return_type = RT_PK
      proto.return_content = return_content
    end

    proto.row_change = serialize_for_put_row(put_row_item.row.primary_key, put_row_item.row.attribute_columns)
    proto.type = PUT
    proto
  end

  def make_update_row_item(proto, update_row_item)
    condition = update_row_item.condition
    if condition.nil?
      condition = Condition.new(RowExistenceExpectation::IGNORE)
    end
    condition_proto = Condition.new
    proto.condition = make_condition(condition_proto, condition)

    if update_row_item.return_type == ReturnType::RT_PK
      return_content = ReturnContent.new
      return_content.return_type = RT_PK
      proto.return_content = return_content
    end
    update_row_item.row.attribute_columns
    proto.row_change = serialize_for_update_row(update_row_item.row.primary_key, update_row_item.row.attribute_columns)
    proto.type = UPDATE
    proto
  end

  def make_delete_row_item(proto, delete_row_item)
    condition = delete_row_item.condition
    if condition.nil?
      condition = Metadata::Condition.new(RowExistenceExpectation::IGNORE)
    end
    condition_proto = Condition.new
    proto.condition = make_condition(condition_proto, condition)

    if delete_row_item.return_type == ReturnType::RT_PK
      return_content = ReturnContent.new
      return_content.return_type = RT_PK
      proto.return_content = return_content
    end

    proto.row_change = serialize_for_delete_row(delete_row_item.row.primary_key)
    proto.type = DELETE
    proto

  end

  def make_repeated_column_names(proto, columns_to_get)
    if columns_to_get.nil?
      return
    end

    columns_to_get.each do |column_name|
      proto << column_name
    end
  end

  def make_condition(proto, condition)
    raise TableStoreClientError.new("condition should be an instance of Condition, not #{condition.class}") unless condition.is_a?(Metadata::Condition)
    expectation_str = condition.row_existence_expectation
    proto.row_existence = expectation_str
    raise TableStoreClientError.new("row_existence_expectation should be one of [#{join(', ')}], not #{expectation_str}") if proto.row_existence.nil?

    if condition.column_condition
      proto.column_condition = make_column_condition(condition.column_condition).serialize_to_string
    end
    proto
  end

  def make_column_condition(column_condition)
    return if column_condition.nil?
    proto = Filter.new
    proto.type = column_condition.get_type

    # condition
    if column_condition.is_a?(Metadata::CompositeColumnCondition)
      proto.filter = make_composite_condition(column_condition)
    elsif column_condition.is_a?(Metadata::SingleColumnCondition)
      proto.filter = make_relation_condition(column_condition)
    else
      raise TableStoreClientError.new("expect CompositeColumnCondition, SingleColumnCondition but not #{column_condition.class}")
    end
    proto
  end

  def make_composite_condition(condition)
    proto = CompositeColumnValueFilter.new
    proto.combinator = condition.get_combinator

    condition.sub_conditions.each do |sub|
      proto.sub_filters << make_column_condition(sub)
    end

    proto.serialize_to_string
  end

  def make_relation_condition(condition)
    proto = SingleColumnValueFilter.new
    proto.comparator = condition.get_comparator

    proto.column_name = condition.get_column_name
    proto.column_value = serialize_column_value(condition.get_column_value)
    proto.filter_if_missing = !condition.pass_if_missing
    proto.latest_version_only = condition.latest_version_only
    proto.serialize_to_string
  end

  def make_schemas_with_list(schema)
    schema_proto = PrimaryKeySchema.new
    schema_proto.name = schema[0]
    schema_proto.type = schema[1]
    if schema.size == 3
      schema_proto.option = 1
    end
    schema_proto
  end

  def make_table_options(options)
    option_proto = TableOptions.new
    unless options.is_a?(Metadata::TableOptions)
      raise TableStoreClientError.new("table_option should be an instance of Meta::TableOptions, not #{options.class}" )
    end
    if options.time_to_live
      unless options.time_to_live.is_a?(Fixnum)
        raise TableStoreClientError("time_to_live should be an instance of int, not #{options.time_to_live.class}")
      end
      option_proto.time_to_live = options.time_to_live
    end
    if options.max_version
      unless options.max_version.is_a?(Fixnum)
        raise TableStoreClientError("max_version should be an instance of int, not #{options.max_version.class}")
      end
      option_proto.max_versions = options.max_version
    end
    if options.max_time_deviation
      unless options.max_time_deviation.is_a?(Fixnum)
        raise TableStoreClientError("max_time_deviation should be an instance of int, not #{options.max_version.class}")
      end
      option_proto.deviation_cell_version_in_sec = options.max_time_deviation
    end
    option_proto
  end

  def make_capacity_unit(capacity_unit)
    proto = CapacityUnit.new
    proto.read = capacity_unit.read if capacity_unit.read
    proto.write = capacity_unit.write if capacity_unit.write
    proto
  end

  ##Parse
  def parse_get_row_item(proto)
    row_list = []
    proto.each do |row_item|
      primary_key_columns = nil
      attribute_columns = nil

      if row_item.is_ok
        # error_code = nil
        # error_message = nil
        # capacity_unit = parse_capacity_unit(row_item.consumed.capacity_unit)

        if row_item.row.length != 0
          inputStream = PlainBufferInputStream.new(row_item.row)
          codedInputStream = PlainBufferCodedInputStream.new(inputStream)
          primary_key_columns, attribute_columns = codedInputStream.read_row
        end
      else
        # error_code = row_item.error.code
        # error_message = row_item.error.HasField('message') ? row_item.error.message : ''
        # if row_item.HasField('consumed')
        #   capacity_unit = parse_capacity_unit(row_item.consumed.capacity_unit)
        # else
        #   capacity_unit = nil
        # end
      end

      row_list << {pk: primary_key_columns, attr: attribute_columns} if primary_key_columns
    end
    row_list
  end

  def parse_batch_write_row(proto)
    result_list = {}
    proto.each do |table_item|
      table_name = table_item.table_name
      result_list[table_name] = []

      table_item.rows.each do |row_item|
        row = parse_write_row_item(row_item)
        result_list[table_name] << row
      end
    end

    result_list
  end

  def parse_write_row_item(row_item)
    primary_key_columns = nil

    if row_item.is_ok
      error_code = nil
      error_message = nil

      if row_item.row.length != 0
        inputStream = PlainBufferInputStream.new(row_item.row)
        codedInputStream = PlainBufferCodedInputStream.new(inputStream)
        primary_key_columns, attribute_columns = codedInputStream.read_row
      end
    end
    primary_key_columns

    #BatchWriteRowResponseItem.new(row_item.is_ok, error_code, error_message, consumed, primary_key_columns)
  end

  private
  def serialize_primary_key(primary_key)
    buf_size = LITTLE_ENDIAN_SIZE
    buf_size += compute_primary_key_size(primary_key)
    buf_size += 2
    output_stream = PlainBufferOutputStream.new(buf_size)
    coded_output_stream = PlainBufferCodedOutputStream.new(output_stream)
    row_checksum = 0
    coded_output_stream.write_header

    row_checksum = coded_output_stream.write_primary_key(primary_key, row_checksum)
    row_checksum = coded_output_stream.crc_int8(row_checksum, 0)
    coded_output_stream.write_row_checksum(row_checksum)
    output_stream.get_buffer.join('')
  end

  def serialize_column_value(value)
    buf_size = compute_variant_value_size(value)
    stream = PlainBufferOutputStream.new(buf_size)
    coded_stream = PlainBufferCodedOutputStream.new(stream)

    coded_stream.write_column_value(value)
    stream.get_buffer.join('')
  end

  def serialize_for_update_row(primary_key, attribute_columns)
    unless attribute_columns.is_a?(Hash)
      raise TableStoreClientError.new("the attribute columns of UpdateRow is not hash, but is #{attribute_columns.class}")
    end

    attribute_columns.keys.each do |key|
      if attribute_columns[key] && !attribute_columns[key].is_a?(Hash)
        raise TableStoreClientError.new("the columns value of update-row must be hash, but is #{attribute_columns[key].class}")
      end
      attribute_columns[key].each do |cell|
        if key.upcase != "DELETE" and key.upcase != "DELETE_ALL" && !cell.is_a?(Array)
          raise TableStoreClientError.new("the cell of update-row must be array, but is #{cell.class}")
        end
      end
    end

    buf_size = compute_update_row_size(primary_key, attribute_columns)
    output_stream = PlainBufferOutputStream.new(buf_size)
    coded_output_stream = PlainBufferCodedOutputStream.new(output_stream)
    row_checksum = 0
    coded_output_stream.write_header
    row_checksum = coded_output_stream.write_primary_key(primary_key, row_checksum)
    row_checksum = coded_output_stream.write_update_columns(attribute_columns, row_checksum)
    row_checksum = PlainBufferCrc8.crc_int8(row_checksum, 0)
    coded_output_stream.write_row_checksum(row_checksum)
    output_stream.get_buffer.join('')
  end

  def compute_variant_value_size(value)
    compute_primary_key_value_size(value) - LITTLE_ENDIAN_SIZE - 1
  end

  def parse_capacity_unit(proto)
    if proto.nil?
      capacity_unit = nil
    else
      cu_read = proto.HasField('read') ? proto.read : 0
      cu_write = proto.HasField('write') ? proto.write : 0
      capacity_unit = CapacityUnit(cu_read, cu_write)
    end
    capacity_unit
  end

  def compute_primary_key_size(primary_key)
    size = 1
    primary_key.each do |pk|
      size += compute_primary_key_column_size(pk[0], pk[1])
    end
    size
  end

  def compute_primary_key_column_size(pk_name, pk_value)
    size = 1
    size += 1 + LITTLE_ENDIAN_SIZE
    size += pk_name.length
    size += compute_primary_key_value_size(pk_value)
    size += 2
    size
  end

  def compute_primary_key_value_size(value)
    size = 1
    size += LITTLE_ENDIAN_SIZE + 1
    if ["INF_MIN", "INF_MAX", "PK_AUTO_INCR"].include?value
      size += 1
      return size
    end
    if value.is_a?(Numeric)
      size += 8
    elsif value.is_a?(String)
      size += LITTLE_ENDIAN_SIZE
      size += value.length
    end
    size
  end

  def serialize_for_put_row(primary_key, attribute_columns)
    buf_size = compute_put_row_size(primary_key, attribute_columns)
    output_stream = PlainBufferOutputStream.new(buf_size)
    coded_output_stream = PlainBufferCodedOutputStream.new(output_stream)

    row_checksum = 0
    coded_output_stream.write_header
    row_checksum = coded_output_stream.write_primary_key(primary_key, row_checksum)
    row_checksum = coded_output_stream.write_columns(attribute_columns, row_checksum)
    row_checksum = PlainBufferCrc8.crc_int8(row_checksum, 0)
    coded_output_stream.write_row_checksum(row_checksum)

    output_stream.get_buffer.join('')
  end

  def serialize_for_delete_row(primary_key)
    buf_size = compute_delete_row_size(primary_key)
    output_stream = PlainBufferOutputStream.new(buf_size)
    coded_output_stream = PlainBufferCodedOutputStream.new(output_stream)

    row_checksum = 0
    coded_output_stream.write_header
    row_checksum = coded_output_stream.write_primary_key(primary_key, row_checksum)
    row_checksum = coded_output_stream.write_delete_marker(row_checksum)
    coded_output_stream.write_row_checksum(row_checksum)

    output_stream.get_buffer.join('')
  end

  ##Compute
  def compute_put_row_size(primary_key, attribute_columns)
    size = LITTLE_ENDIAN_SIZE
    size += compute_primary_key_size(primary_key)

    if attribute_columns.length != 0
      size += 1
      attribute_columns.each do |attr|
        if attr.length == 2
          size += compute_column_size(attr[0], attr[1])
        else
          size += compute_column_size(attr[0], attr[1], attr[2])
        end
      end
    end
    size += 2
    size
  end

  def compute_column_size(column_name, column_value, timestamp = nil)
    size = 1
    size += 1 + LITTLE_ENDIAN_SIZE
    size += column_name.length
    unless column_value.nil?
      size += compute_column_value_size(column_value)
    end
    unless timestamp.nil?
      size += 1 + LITTLE_ENDIAN_64_SIZE
    end
    size += 2
    size
  end

  def compute_column_value_size(value)
    size = 1
    size += LITTLE_ENDIAN_SIZE + 1

    if value.is_a?(TrueClass) || value.is_a?(FalseClass)
      size += 1
    elsif value.is_a?(Fixnum)
      size += LITTLE_ENDIAN_64_SIZE
    elsif value.is_a?(String)
      size += LITTLE_ENDIAN_SIZE
      size += value.length
    elsif value.is_a?(Float)
      size += LITTLE_ENDIAN_64_SIZE
    else
      raise TableStoreClientError.new("Unsupported column type: #{value.class}" )
    end
    size
  end

  def compute_update_row_size(primary_key, attribute_columns)
    size = LITTLE_ENDIAN_SIZE
    size += compute_primary_key_size(primary_key)
    if attribute_columns.length != 0
      size += 1
      attribute_columns.keys.each do |update_type|
        columns = attribute_columns[update_type]
        if columns.is_a?(String)
          size += compute_column_size2(column, nil, update_type)
        elsif columns.is_a?(Hash)
          columns.each do |column|
            if column.length == 1
              size += compute_column_size2(column[0], nil, update_type)
            elsif column.length >= 2
              size += compute_column_size2(column[0], column[1], update_type)
            else
              raise OTSClientError("Unsupported column type:#{columns.class}")
            end
          end
        end
      end
    end
    size += 2
    size
  end

  def compute_column_size2(column_name, column_value, update_type)
    size = compute_column_size(column_name, column_value)
    if update_type == "DELETE" || update_type == "DELETE_ALL"
      size += 2
    end
    size
  end

  def compute_delete_row_size(primary_key)
    size = LITTLE_ENDIAN_SIZE
    size += compute_primary_key_size(primary_key)
    size += 3
    size
  end
end