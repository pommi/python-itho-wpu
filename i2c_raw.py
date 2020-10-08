import io
import fcntl
import struct

I2C_SLAVE = 0x0703


class I2CRaw:
    def __init__(self, address, bus):
        self.fr = io.open(f"/dev/i2c-{bus}", "rb", buffering=0)
        self.fw = io.open(f"/dev/i2c-{bus}", "wb", buffering=0)
        fcntl.ioctl(self.fr, I2C_SLAVE, address)
        fcntl.ioctl(self.fw, I2C_SLAVE, address)

    def write_i2c_block_data(self, data):
        if type(data) is not list:
            return -1
        data = bytearray(data)
        self.fw.write(data)
        return 0

    def read_i2c_block_data(self, n_bytes):
        data_raw = self.fr.read(n_bytes)
        unpack_format = 'B'*n_bytes
        return list(struct.unpack(unpack_format, data_raw))

    def close(self):
        self.fr.close()
        self.fw.close()
