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

def list_table(client)
  client._list_table
end

def update_table(client)
  table_option = Metadata::TableOptions.new(time_to_live=96000, max_version= 2, max_time_deviation= 100000)
  client._update_table($table_name, table_option)
end

def delete_table(client)
  client._delete_table($table_name)
end

client = TableStore.new(BaseUrl, UserID, UserSecret, InstanceName)
p create_table(client)
p list_table(client)
p update_table(client)
p delete_table(client)


