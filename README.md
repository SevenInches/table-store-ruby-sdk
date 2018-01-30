TableStore SDK for Ruby
=======================

概述
----

- 此Ruby SDK基于 `阿里云表格存储服务 <http://www.aliyun.com/product/ots/>`_  API构建

安装方法
---------

#### 1.GEM 安装 
.. code-block:: bash

    $ gem install tablestore-ruby-sdk 
或者在gem中引入
          
    $ gem 'tablestore-ruby-sdk'

#### 2.Github安装    
.. code-block:: bash
        
    $ git clone https://github.com/SevenInches/table-store-ruby-sdk.git
 
示例代码
---------

- `表操作（表的创建、获取、更新和删除） <https://github.com/SevenInches/table-store-ruby-sdk/examples/tables_demo.rb>`_
- `单行写（向表内写入一行数据） <https://github.com/SevenInches/table-store-ruby-sdk/examples/rows_demo.rb>`_
- `单行读（从表内读出一样数据） <https://github.com/SevenInches/table-store-ruby-sdk/examples/rows_demo.rb>`_
- `更新单行（更新某一行的部分字段） <https://github.com/SevenInches/table-store-ruby-sdk/examples/rows_demo.rb>`_
- `删除某行（从表内删除某一行数据） <https://github.com/SevenInches/table-store-ruby-sdk/examples/rows_demo.rb>`_
- `批量写（向多张表，一次性写入多行数据） <https://github.com/SevenInches/table-store-ruby-sdk/examples/batch_demo.rb>`_
- `批量读（从多张表，一次性读出多行数据） <https://github.com/SevenInches/table-store-ruby-sdk/examples/batch_demo.rb>`_
- `范围扫描（给定一个范围，扫描出该范围内的所有数据） <https://github.com/SevenInches/table-store-ruby-sdk/examples/batch_demo.rb>`_
- `主键自增列（主键自动生成一个递增ID） <https://github.com/aliyun/aliyun-tablestore-python-sdk/blob/master/examples/pk_auto_incr.py>`_    

执行测试
---------
**注意：测试case中会有清理某个实例下所有表的动作，所以请使用专门的测试实例来测试。**

#### 1.需要在每一个demo里设置examples的配置
.. code-block:: bash
    
    BaseUrl = <tablestore service endpoint>
    UserID = <your access id>
    UserSecret = <your access key>
    InstanceName = <your instance name>
    $table_name = <your operation table>

或设置在application.yml中，并引入
    
    
#### 2.执行demo
.. code-block:: bash

    $ ruby examples/<you test demo file>
