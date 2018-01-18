require 'base64'

class OTSProtocol
  $api_version = '2015-12-31'
  $user_agent = 'table-store-ruby'

  encoder_class = OTSProtoBufferEncoder.new
  decoder_class = OTSProtoBufferDecoder.new

  $api_list = [
      'CreateTable',
      'ListTable',
      'DeleteTable',
      'DescribeTable',
      'UpdateTable',
      'GetRow',
      'PutRow',
      'UpdateRow',
      'DeleteRow',
      'BatchGetRow',
      'BatchWriteRow',
      'GetRange'
  ]

  def initialize(user_id, user_key, sts_token, instance_name, encoding, logger)
    @user_id = user_id
    @user_key = user_key
    @sts_token = sts_token
    @instance_name = instance_name
    @encoding = encoding
    @encoder = encoder_class(encoding)
    @decoder = decoder_class(encoding)
    @logger = logger
  end

  def make_headers_string(headers)
    headers.map{|k,v| "#{k.downcase}:#{v.strip}" if k.include?('x-ots-') and k != 'x-ots-signature'}.sort.join('\n')
  end

  def call_signature_method(signature_string)
    salt = Digest::SHA1.digest(signature_string)
    Base64.encode64(HMAC::SHA1.digest(UserSecret, salt)).strip
  end

  def make_request_signature(query, headers)
    signature_string = query + '\n' + 'POST' + '\n\n'

    headers_string = make_headers_string(headers)
    signature_string += headers_string + '\n'
    call_signature_method(signature_string)
  end

  def make_headers(body, query)
    md5 = Base64.encode64(Digest::MD5.new.digest(body)).gsub(/\n/, '')
    date = Time.now.strftime('%Y-%m-%dT%H:%M:%S.000Z')

    headers = {
      'x-ots-date': date,
      'x-ots-apiversion': $api_version,
      'x-ots-accesskeyid': @user_id,
      'x-ots-instancename': @instance_name,
      'x-ots-contentmd5': md5,
    }

    headers['x-ots-ststoken'] = @sts_token if @sts_token

    signature = make_request_signature(query, headers)
    headers['x-ots-signature'] = signature
    headers['User-Agent'] = $user_agent
    headers
  end

  def make_response_signature(query, headers)
    uri = query
    headers_string = make_headers_string(headers)

    signature_string = headers_string + '\n' + uri
    signature = call_signature_method(signature_string)
    signature
  end

  def check_headers(headers, body, status=nil)
      # check the response headers and process response body if needed.

      header_names = [
          'x-ots-contentmd5',
          'x-ots-requestid',
          'x-ots-date',
          'x-ots-contenttype',
      ]

    if status >= 200 and status < 300
      header_names.each do |name|
        raise TableStoreClientError.new("#{name} is missing in response header.") unless headers.include?name

        if headers.include?'x-ots-contentmd5'
          md5 = Base64.encode64(Digest::MD5.new.digest(body)).gsub(/\n/, '')
        end
        raise TableStoreClientError.new('MD5 mismatch in response.')  if md5 != headers['x-ots-contentmd5']
      end
    end
  end

  def check_authorization(query, headers, status=nil)
    auth = headers.get('authorization')
    if auth.nil?
      if status >= 200 and status < 300
        raise TableStoreClientError.new('"Authorization" is missing in response header.')
      else
        return
      end
    end
    # 1, check authorization
    unless auth.include?('OTS ')
      raise TableStoreClientError.new('Invalid Authorization in response.')
    end
    # 2, check accessid
    access_id, signature = auth[4..-1].split(':')
    if access_id != self.user_id
      raise TableStoreClientError.new('Invalid accesskeyid in response.')
    end
    # 3, check signature
    # decode the byte type
    if signature != make_response_signature(query, headers)
      raise TableStoreClientError.new('Invalid signature in response.')
    end
  end

  def make_request(api_name)
    raise TableStoreClientError("API #{api_name} is not supported.") if $api_list.exclude?api_name
    body = encoder.encode_request(api_name, *args)
    query = '/' + api_name
    headers = make_headers(body, query)
    return query, headers, body
  end

  def get_request_id_string(headers)
    request_id = headers.get('x-ots-requestid')
    request_id = ""  if request_id.nil?
    request_id
  end

  def parse_response(api_name, status, headers, body)
    raise TableStoreClientError.new("API #{api_name} is not supported.") unless api_list.include?(api_name)
    begin
      hash, proto = decoder.decode_response(api_name, body)
    rescue => e
      request_id = get_request_id_string(headers)
      error_message = "Response format is invalid, #{e}, RequestID: #{request_id}, " \
                "HTTP status: #{status}, Body: #{body}."
      self.logger.error(error_message)
      raise TableStoreClientError.new(error_message, status)
    end
    hash
  end

  # def handle_error(api_name, query, status, reason, headers, body)
  #
  # end

end