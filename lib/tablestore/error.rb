
class TableStoreError

end

class TableStoreClientError < ::StandardError
  attr_accessor :message
  attr_accessor :http_status

  def initialize(message, http_status = nil)
    @message = message
    @http_status = http_status
  end

  def get_http_status
    @http_status
  end

  def get_error_message
    @message
  end
end

class TableStoreServiceError < ::StandardError
  attr_accessor :message
  attr_accessor :http_status
  attr_accessor :code
  attr_accessor :request_id

  def initialize(http_status, code, message, request_id = '')
    @http_status = http_status
    @code = code
    @message = message
    @request_id = request_id
  end

  def self.string
    "ErrorCode: #{@code}, ErrorMessage: #{@message}, RequestID: #{@request_id}"
  end

  def get_http_status
    @http_status
  end

  def get_error_message
    @message
  end

  def get_error_code
    @code
  end

  def get_request_id
    @request_id
  end
end

