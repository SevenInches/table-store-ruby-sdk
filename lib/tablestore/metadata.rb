
ALL = [
    'INF_MIN',
    'INF_MAX',
    'PK_AUTO_INCR',
    'TableMeta',
    'TableOptions',
    'CapacityUnit',
    'ReservedThroughput',
    'ReservedThroughputDetails',
    'ColumnType',
    'ReturnType',
    'Column',
    'Direction',
    'UpdateType',
    'UpdateTableResponse',
    'DescribeTableResponse',
    'RowDataItem',
    'Condition',
    'Row',
    'RowItem',
    'PutRowItem',
    'UpdateRowItem',
    'DeleteRowItem',
    'BatchGetRowRequest',
    'TableInBatchGetRowItem',
    'BatchGetRowResponse',
    'BatchWriteRowType',
    'BatchWriteRowRequest',
    'TableInBatchWriteRowItem',
    'BatchWriteRowResponse',
    'BatchWriteRowResponseItem',
    'LogicalOperator',
    'ComparatorType',
    'ColumnConditionType',
    'ColumnCondition',
    'CompositeColumnCondition',
    'SingleColumnCondition',
    'RowExistenceExpectation',
]

module Metadata
  class TableMeta
    attr_accessor :table_name, :schema_of_primary_key
    def initialize(table_name, schema_of_primary_key)
      # schema_of_primary_key: [('PK0', 'STRING'), ('PK1', 'INTEGER'), ...]
      self.table_name = table_name
      self.schema_of_primary_key = schema_of_primary_key
    end
  end

  class TableOptions
    attr_accessor :time_to_live, :max_version, :max_time_deviation
    def initialize(time_to_live = -1, max_version = 1, max_time_deviation = 86400)
      self.time_to_live = time_to_live
      self.max_version = max_version
      self.max_time_deviation = max_time_deviation
    end
  end

  class CapacityUnit
    attr_accessor :read, :write
    def initialize(read=0, write=0)
      self.read = read
      self.write = write
    end
  end

  class ReservedThroughput
    attr_accessor :capacity_unit
    def initialize(capacity_unit)
       self.capacity_unit = capacity_unit
    end
  end

  class RowExistenceExpectation
    IGNORE = "IGNORE"
    EXPECT_EXIST = "EXPECT_EXIST"
    EXPECT_NOT_EXIST = "EXPECT_NOT_EXIST"

    VALUES = [
        IGNORE,
        EXPECT_EXIST,
        EXPECT_NOT_EXIST,
    ]

    MEMBERS = [
        "RowExistenceExpectation::IGNORE",
        "RowExistenceExpectation::EXPECT_EXIST",
        "RowExistenceExpectation::EXPECT_NOT_EXIST",
    ]
  end

  class LogicalOperator
    NOT = 1
    AND = 2
    OR = 3

    VALUES = [
      NOT,
      AND,
      OR
    ]

    MEMBERS = [
      "LogicalOperator::NOT",
      "LogicalOperator::AND",
      "LogicalOperator::OR"
    ]
  end

  class ComparatorType
    EQUAL = 1
    NOT_EQUAL = 2
    GREATER_THAN = 3
    GREATER_EQUAL = 4
    LESS_THAN = 5
    LESS_EQUAL = 6

    VALUES = [
      EQUAL,
      NOT_EQUAL,
      GREATER_THAN,
      GREATER_EQUAL,
      LESS_THAN,
      LESS_EQUAL,
    ]

    MEMBERS = [
      "ComparatorType::EQUAL",
      "ComparatorType::NOT_EQUAL",
      "ComparatorType::GREATER_THAN",
      "ComparatorType::GREATER_EQUAL",
      "ComparatorType::LESS_THAN",
      "ComparatorType::LESS_EQUAL",
    ]
  end

  class Condition
    attr_accessor :row_existence_expectation, :column_condition

    def initialize(row_existence_expectation, column_condition = nil)
      self.row_existence_expectation = nil
      self.column_condition = column_condition

      set_row_existence_expectation(row_existence_expectation)
    end

    def set_row_existence_expectation(row_existence_expectation)
      raise TableStoreClientError.new("Expect input row_existence_expectation should be one of #{RowExistenceExpectation::MEMBERS.to_s}, but #{row_existence_expectation}")  unless RowExistenceExpectation::VALUES.include? row_existence_expectation
      self.row_existence_expectation = row_existence_expectation
    end

  end

  class Row
    attr_accessor :primary_key, :attribute_columns
    def initialize(primary_key, attribute_columns=nil)
      self.primary_key = primary_key
      self.attribute_columns = attribute_columns
    end

  end

  class RowItem
    attr_accessor :type, :row, :condition, :return_type
    def initialize(row_type, row, condition, return_type = nil)
      self.type = row_type
      self.condition = condition
      self.row = row
      self.return_type = return_type
    end
  end

  class ColumnConditionType
    COMPOSITE_COLUMN_CONDITION = 2
    SINGLE_COLUMN_CONDITION = 1
  end

  class CompositeColumnCondition
    def initialize(combinator)
      @sub_conditions = []
      set_combinator(combinator)
    end

    def get_type
      ColumnConditionType::COMPOSITE_COLUMN_CONDITION
    end

    def set_combinator(combinator)
      unless LogicalOperator::VALUES.include?combinator
        raise TableStoreClientError.new("Expect input combinator should be one of #{LogicalOperator::MEMBERS.to_s}, but #{combinator}")
      end
      @combinator = combinator
    end

    def get_combinator
      @combinator
    end

    def add_sub_condition(condition)
      @sub_conditions<< condition
    end

    def clear_sub_condition
      @sub_conditions = []
    end

    def sub_conditions
      @sub_conditions
    end
  end

  class SingleColumnCondition
    def initialize(column_name, column_value, comparator, pass_if_missing = true, latest_version_only = true)
      @column_name = column_name
      @column_value = column_value

      @comparator = nil
      @pass_if_missing = nil
      @latest_version_only = nil

      set_comparator(comparator)
      set_pass_if_missing(pass_if_missing)
      set_latest_version_only(latest_version_only)
    end

    def get_type
       ColumnConditionType::SINGLE_COLUMN_CONDITION
    end

    def set_pass_if_missing(pass_if_missing)
      @pass_if_missing = pass_if_missing
    end

    def get_pass_if_missing
      @pass_if_missing
    end

    def set_latest_version_only(latest_version_only)
      @latest_version_only = latest_version_only
    end

    def get_latest_version_only
      @latest_version_only
    end

    def set_column_name(column_name)
      if column_name.nil?
        raise TableStoreClientError.new("The input column_name of SingleColumnCondition should not be None")
      end
      @column_name = column_name
    end

    def get_column_name
      @column_name
    end

    def set_column_value(column_value)
      if column_value.nil
        raise TableStoreClientError.new("The input column_value of SingleColumnCondition should not be None")
      end
      @column_value = column_value
    end

    def get_column_value
      @column_value
    end

    def set_comparator(comparator)
      @comparator = comparator
    end

    def get_comparator
      @comparator
    end

    def pass_if_missing
      @pass_if_missing
    end

    def latest_version_only
      @latest_version_only
    end
  end

  class BatchGetRowRequest
    def initialize
      @items = {}
    end

    def items
      @items
    end

    def add(table_item)
        """
        说明：添加tablestore.metadata.TableInBatchGetRowItem对象
        注意：对象内部存储tablestore.metadata.TableInBatchGetRowItem对象采用‘字典’的形式，Key是表
              的名字，因此如果插入同表名的对象，那么之前的对象将被覆盖。
        """
      unless table_item.is_a?(TableInBatchGetRowItem)
        raise TableStoreClientError.new("The input table_item should be an instance of TableInBatchGetRowItem, not #{table_item.class}")
      end
      @items[table_item.table_name] = table_item
    end
  end

  class TableInBatchWriteRowItem
    attr_accessor :table_name, :row_items
    def initialize(table_name, row_items)
      self.table_name = table_name
      self.row_items = row_items
    end
  end

  class BatchWriteRowRequest
    attr_accessor :items

    def initialize
      self.items = {}
    end

    def add(table_item)
        """
        说明：添加tablestore.metadata.TableInBatchWriteRowItem对象
        注意：对象内部存储tablestore.metadata.TableInBatchWriteRowItem对象采用‘字典’的形式，Key是表
              的名字，因此如果插入同表名的对象，那么之前的对象将被覆盖。
        """
        unless table_item.is_a?(TableInBatchWriteRowItem)
          raise TableStoreClientError.new("The input table_item should be an instance of TableInBatchWriteRowItem, not #{table_item.class}")
        end

      self.items[table_item.table_name] = table_item
    end

  end

  class TableInBatchGetRowItem
    def initialize(table_name, primary_keys, columns_to_get=nil,
          column_filter=nil, max_version=nil, time_range=nil,
          start_column=nil, end_column=nil, token=nil)
      @table_name = table_name
      @primary_keys = primary_keys
      @columns_to_get = columns_to_get
      @column_filter = column_filter
      @max_version = max_version
      @time_range = time_range
      @start_column = start_column
      @end_column = end_column
      @token = token
    end

    def table_name
      @table_name
    end

    def primary_keys
      @primary_keys
    end

    def columns_to_get
      @columns_to_get
    end

    def column_filter
      @column_filter
    end

    def max_version
      @max_version
    end

    def time_range
      @time_range
    end

    def start_column
      @start_column
    end

    def end_column
      @end_column
    end

    def token
      @token
    end
  end

  class BatchWriteRowType
    PUT = "put"
    UPDATE = "update"
    DELETE = "delete"
  end

  class INF_MIN
  end

  class INF_MAX
  end

  class PK_AUTO_INCR
  end
end

