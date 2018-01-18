
VERSION = '0.0.1'
ALL_DATA_TYPE = [
    'OTSClient',
    # Data Types
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
    'TableStoreClientError',
    'TableStoreServiceError',
    'DefaultRetryPolicy',
    'LogicalOperator',
    'ComparatorType',
    'ColumnConditionType',
    'ColumnCondition',
    'CompositeColumnCondition',
    'SingleColumnCondition',
    'RowExistenceExpectation',
]

require 'client'
require 'metadata'
require 'error'
require 'retry'
require 'const'