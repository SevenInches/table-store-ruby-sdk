require 'tablestore-ruby-sdk'
BaseUrl = ''
UserID = ''
UserSecret = ''
InstanceName = ''
$table_name = 'test_table'

def create_table(client)
  schema_of_primary_key = [['gid', 'INTEGER'], ['uid', 'STRING']]
  table_meta = Metadata::TableMeta.new($table_name, schema_of_primary_key)
  table_option = Metadata::TableOptions.new
  reserved_throughput = Metadata::ReservedThroughput.new(Metadata::CapacityUnit.new(0, 0))
  client._create_table(table_meta, table_option, reserved_throughput)
end

def batch_write_row(client)
  put_row_items = []
  (1..20).each do |i|
    primary_key = {'gid': i, 'uid': (64+i).chr}
    attribute_columns = {'name': "somebody#{i}", 'address':"somewhere#{i}", 'age': i}
    row = Metadata::Row.new(primary_key, attribute_columns)
    condition = Metadata::Condition.new(Metadata::RowExistenceExpectation::IGNORE)
    item = Metadata::RowItem.new(Metadata::BatchWriteRowType::PUT, row, condition)
    put_row_items << item
  end
  request = Metadata::BatchWriteRowRequest.new
  request.add(Metadata::TableInBatchWriteRowItem.new($table_name, put_row_items))
  client._batch_write_row(request)
end

def batch_update_row(client)
  update_row_items = []
  (1..20).each do |i|
    primary_key = {'gid': i, 'uid': (64+i).chr}
    attribute_columns = {'put': {'name': "nobody#{i}", 'address':"nowhere#{i}", 'age': i+1, 'country':'China'}}
    row =  Metadata::Row.new(primary_key, attribute_columns)
    condition = Metadata::Condition.new(Metadata::RowExistenceExpectation::EXPECT_EXIST)
    item = Metadata::RowItem.new(Metadata::BatchWriteRowType::UPDATE, row, condition)
    update_row_items << item
  end
  request = Metadata::BatchWriteRowRequest.new
  request.add(Metadata::TableInBatchWriteRowItem.new($table_name, update_row_items))
  client._batch_write_row(request)
end

def get_range(client)
  cond = Metadata::CompositeColumnCondition.new(Metadata::LogicalOperator::AND)
  cond.add_sub_condition(Metadata::SingleColumnCondition.new("country", 'China', Metadata::ComparatorType::EQUAL))
  cond.add_sub_condition(Metadata::SingleColumnCondition.new("age", 10, Metadata::ComparatorType::LESS_THAN))

  request = {
      'table_name': $table_name,
      'direction': :FORWARD,
      'inclusive_start_primary_key': {'gid': Metadata::INF_MIN, 'uid': Metadata::INF_MIN},
      'exclusive_end_primary_key': {'gid': Metadata::INF_MAX, 'uid': Metadata::INF_MAX},
      'limit': 10,
      'max_version': 1,
      'column_filter': cond
  }
  client._get_range(request) #next_start_primary_key, row_list
end

def batch_get_row(client)
  columns_to_get = []
  rows_to_get = []
  (1..20).each do |i|
    primary_key = {'gid': i, 'uid': (64+i).chr}
    rows_to_get << primary_key
  end
  cond = Metadata::CompositeColumnCondition.new(Metadata::LogicalOperator::AND)
  cond.add_sub_condition(Metadata::SingleColumnCondition.new("country", 'China', Metadata::ComparatorType::EQUAL))
  cond.add_sub_condition(Metadata::SingleColumnCondition.new("age", 10, Metadata::ComparatorType::LESS_THAN))
  cond.add_sub_condition(Metadata::SingleColumnCondition.new("age", 2, Metadata::ComparatorType::GREATER_THAN))
  request = Metadata::BatchGetRowRequest.new
  request.add(Metadata::TableInBatchGetRowItem.new($table_name, rows_to_get, columns_to_get, cond, 1))

  client._batch_get_row(request)
end

def batch_delete_row(client)
  delete_row_items = []
  (1..20).each do |i|
    primary_key = {'gid': i, 'uid': (64+i).chr}
    row =  Metadata::Row.new(primary_key)
    condition = Metadata::Condition.new(Metadata::RowExistenceExpectation::IGNORE)
    item = Metadata::RowItem.new(Metadata::BatchWriteRowType::DELETE, row, condition)
    delete_row_items << item
  end
  request = Metadata::BatchWriteRowRequest.new
  request.add(Metadata::TableInBatchWriteRowItem.new($table_name, delete_row_items))
  client._batch_write_row(request)
end

def delete_table(client)
  client._delete_table($table_name)
end

client = TableStore.new(BaseUrl, UserID, UserSecret, InstanceName)
p create_table(client)
sleep(1)
p batch_write_row(client)
p batch_update_row(client)
p get_range(client)
p batch_get_row(client)
p batch_delete_row(client)
p delete_table(client)