require 'tablestore/crc8_auto'

class PlainBufferCrc8
  class << self
    def crc_string(crc, bytes)
      Digest::CRC8Auto.new.crc8(crc, bytes.to_s).checksum
    end

    def crc_int8(crc, byte)
      Digest::CRC8Auto.new.crc8(crc, [byte].pack('C*')).checksum
    end

    def crc_int32(crc, byte)
      Digest::CRC8Auto.new.crc8(crc, [byte].pack('i')).checksum
    end

    def crc_int64(crc, byte)
      Digest::CRC8Auto.new.crc8(crc, [byte].pack('q')).checksum
    end
  end
end
