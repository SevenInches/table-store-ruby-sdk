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

def put_row(client)
  primary_key = {'gid':1, 'uid': 'DF101'}
  attribute_columns = {:car_brand=>"宝马", :is_master=>true, :age=>20, :name=>'John'}
  row = Metadata::Row.new(primary_key, attribute_columns)
  condition = Metadata::Condition.new(Metadata::RowExistenceExpectation::IGNORE)
  client._put_row($table_name, row, condition)
end

def get_row(client)
  primary_key = {'gid':1, 'uid': 'DF101'}
  columns_to_get = [] # given a list of columns to get, or empty list if you want to get entire row.

  cond = Metadata::CompositeColumnCondition.new(Metadata::LogicalOperator::AND)
  cond.add_sub_condition(Metadata::SingleColumnCondition.new("age", 9, Metadata::ComparatorType::GREATER_THAN))
  cond.add_sub_condition(Metadata::SingleColumnCondition.new("name", 'John', Metadata::ComparatorType::EQUAL))
  client._get_row($table_name, primary_key, columns_to_get, cond, 1)
end

def delete_row(client)
  primary_key = {'gid':1, 'uid':'DF101'}
  row =   Metadata::Row.new(primary_key)
  condition =  Metadata::Condition.new( Metadata::RowExistenceExpectation::IGNORE,  Metadata::SingleColumnCondition.new("age", 25,  Metadata::ComparatorType::LESS_THAN))
  client._delete_row($table_name, row, condition)
end

def delete_table(client)
  client._delete_table($table_name)
end

client = TableStore.new(BaseUrl, UserID, UserSecret, InstanceName)
p create_table(client)
p put_row(client)
p get_row(client)
p delete_row(client)
p delete_table(client)


