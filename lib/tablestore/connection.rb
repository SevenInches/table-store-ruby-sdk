require 'rest-client'
require 'tablestore/error'

NETWORK_IO_TIME_COUNT_FLAG = false
NETWORK_IO_TIME = 0

class ConnectionPool
  NUM_POOLS = 5

  def initialize(host, path, timeout=0, maxsize=50)
    @host = host
    @path = path
  end

  def send_receive(url, request_headers, request_body)
    response = RestClient.post(url, request_body, request_headers)
    response_headers = response.headers
    response_body = response.body

    return response.status, response_headers, response_body
  end
end