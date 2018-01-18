require 'rest-client'
require 'openssl'
require 'base64'
require 'tablestore/error'
require 'tablestore/table_store_client'
require 'tablestore/metadata'

class TableStore
  DEFAULT_ENCODING = 'utf8'
  DEFAULT_SOCKET_TIMEOUT = 50
  DEFAULT_MAX_CONNECTION = 50
  DEFAULT_LOGGER_NAME = 'tablestore-client'

  attr_accessor :base_url, :access_key_id, :access_key_secret, :instance_name

  def initialize(base_url, access_key_id, access_key_secret, instance_name, **kwargs)
    # 初始化TableStoreClient实例。
    # end_point是TableStoreClient服务的地址（例如 'http://instance.cn-hangzhou.TableStoreClient.aliyun.com'），必须以'http://'或'https://'开头。
    # access_key_id是访问TableStoreClient服务的accessid，通过官方网站申请或通过管理员获取。
    # access_key_secret是访问TableStoreClient服务的accesskey，通过官方网站申请或通过管理员获取。
    # instance_name是要访问的实例名，通过官方网站控制台创建或通过管理员获取。
    # sts_token是访问TableStoreClient服务的STS token，从STS服务获取，具有有效期，过期后需要重新获取。
    # encoding请求参数的字符串编码类型，默认是utf8。
    # socket_timeout是连接池中每个连接的Socket超时，单位为秒，可以为int或float。默认值为50。
    # max_connection是连接池的最大连接数。默认为50，
    # logger_name用来在请求中打DEBUG日志，或者在出错时打ERROR日志。
    # retry_policy定义了重试策略，默认的重试策略为 DefaultRetryPolicy。你可以继承 RetryPolicy 来实现自己的重试策略，请参考 DefaultRetryPolicy 的代码。

    #self.validate_parameter(end_point, access_key_id, access_key_secret, instance_name)
    #sts_token = kwargs.get('sts_token')

    self.base_url            = base_url
    self.access_key_id       = access_key_id
    self.access_key_secret   = access_key_secret
    self.instance_name       = instance_name

    #示例：创建一个TableStoreClient实例
    # from tablestore.client import TableStoreClient
    # client = TableStoreClient('your_instance_endpoint', 'your_user_id', 'your_user_key', 'your_instance_name')
  end

  ##tables
  def _create_table(table_meta, table_option, reserved_throughput)
    api_name = 'CreateTable'
    body = TableStoreClient.new.encode_create_table(table_meta, table_option, reserved_throughput)
    response = post_request(body, api_name)
    if response.code == 200
      "create table #{table_meta.table_name} succeed!"
    end
  end

  def _list_table
    api_name = "ListTable"
    body = TableStoreClient.new.encode_list_table
    response = post_request(body, api_name)
    TableStoreClient.new.decode_list_table(response.body)
  end

  def _update_table(table_name, table_option, reserved_throughput=nil)
    api_name = 'UpdateTable'
    body = TableStoreClient.new.encode_update_table(table_name, table_option, reserved_throughput)
    response = post_request(body, api_name)
    if response.code == 200
      "update table #{table_name} succeed!"
    end
  end

  def _delete_table(table_name)
    api_name = 'DeleteTable'
    body = TableStoreClient.new.encode_delete_table(table_name)
    response = post_request(body, api_name)
    if response.code == 200
      "delete table #{table_name} succeed!"
    end
  end

  ##rows
  def _get_range(request)
    api_name = 'GetRange'
    body = TableStoreClient.new.encode_get_range_request(request)
    response = post_request(body, api_name)
    TableStoreClient.new.decode_get_range_request(response.body)
  end

  def _put_row(table_name, row, condition)
    api_name = 'PutRow'
    body = TableStoreClient.new.encode_put_row(table_name, row, condition)
    response = post_request(body, api_name)
    if response.code == 200
      'write succeed!'
    end
  end

  def _get_row(table_name, primary_key, columns_to_get=nil, column_filter=nil, max_version=1)
    api_name = 'GetRow'
    body = TableStoreClient.new.encode_get_row(table_name, primary_key, columns_to_get, column_filter, max_version)
    response = post_request(body, api_name)
    TableStoreClient.new.decode_get_row(response.body)
  end

  def _update_row(table_name, row, condition)
    api_name = 'UpdateRow'
    body = TableStoreClient.new.encode_update_row(table_name, row, condition)
    response = post_request(body, api_name)
    if response.code == 200
      'update succeed!'
    end
  end

  def _delete_row(table_name, row, condition)
    api_name = 'DeleteRow'
    body = TableStoreClient.new.encode_delete_row(table_name, row, condition)
    response = post_request(body, api_name)
    if response.code == 200
      'delete succeed!'
    end
  end

  def _batch_get_row(request)
    api_name = 'BatchGetRow'
    body = TableStoreClient.new.make_batch_get_row(request)
    response = post_request(body, api_name)
    TableStoreClient.new.decode_batch_get_row(response.body)
  end

  def _batch_write_row(request)
    api_name = 'BatchWriteRow'
    body = TableStoreClient.new.make_batch_write_row(request)
    response = post_request(body, api_name)
    if response.code == 200
      'write succeed!'
    end
  end

  private
  def post_request(body, api_name)
    md5 = Base64.encode64(Digest::MD5.new.digest(body)).gsub(/\n/, '')
    headers = get_headers(md5, api_name)
    url = base_url + '/' + api_name
    begin
      RestClient.post(url, body, headers)
    rescue RestClient::ExceptionWithResponse => e
      raise e.response
    end
  end

  def get_headers(md5, api_name)
    headers = {
        "x-ots-date": Time.now.getutc.strftime('%Y-%m-%dT%H:%M:%S.000Z'),
        "x-ots-apiversion": '2015-12-31',
        "x-ots-accesskeyid": access_key_id,
        "x-ots-contentmd5": md5,
        "x-ots-instancename": instance_name,
    }
    signature_string = "/#{api_name}\nPOST\n\n"
    headers_string = headers.map{|k,v| "#{k.downcase}:#{v.strip}"}.sort.join("\n")
    signature_string += headers_string + "\n"
    salt1 = OpenSSL::HMAC.digest('sha1', access_key_secret, signature_string)
    signature = Base64.encode64(salt1).gsub(/\n/, '')
    headers.merge!({'User-Agent': 'aliyun-tablestore-sdk-ruby', 'x-ots-signature': signature, "Content-Type": 'application/x-www-form-urlencoded',})
    headers
  end
end