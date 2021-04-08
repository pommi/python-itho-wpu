import fcntl
import io
import logging
import pigpio
import struct
import time

logger = logging.getLogger(__name__)

actions = {
    "getnodeid": [0x90, 0xE0],
    "getserial": [0x90, 0xE1],
    "getdatatype": [0xA4, 0x00],
    "getdatalog": [0xA4, 0x01],
}


class I2CRaw:
    def __init__(self, address, bus):
        I2C_SLAVE = 0x0703
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


class I2CMaster:
    def __init__(self, address, bus, queue):
        self.i = I2CRaw(address=address, bus=bus)
        self.queue = queue

    def compose_request(self, action):
        # 0x80 = source, 0x04 = msg_type, 0x00 = length
        request = [0x80] + actions[action] + [0x04, 0x00]
        request.append(self.calculate_checksum(request))
        return request

    def calculate_checksum(self, request):
        s = 0x82
        for i in request:
            s += i
        checksum = 256 - (s % 256)
        if checksum == 256:
            checksum = 0
        return checksum

    def execute_action(self, action):
        request = self.compose_request(action)
        result = None
        for i in range(0, 20):
            logger.debug(f"Executing action: {action}")
            self.i.write_i2c_block_data(request)
            time.sleep(0.21)
            logger.debug("Queue size: {}".format(self.queue.qsize()))
            if self.queue.qsize() > 0:
                result = self.queue.get()
                break

        if result is None:
            logger.error("No valid result in 20 requests")
        return result

    def close(self):
        self.i.close()


class I2CSlave():
    def __init__(self, address, queue):
        self.address = address
        self.queue = queue
        self.pi = pigpio.pi()
        if not self.pi.connected:
            logger.error("not pi.connected")
            return

    def set_callback(self):
        logger.debug("set_callback()")
        self.event_callback = self.pi.event_callback(pigpio.EVENT_BSC, self.callback)
        self.pi.bsc_i2c(self.address)

    def callback(self, id, tick):
        logger.debug(f"callback({id}, {tick})")
        s, b, d = self.pi.bsc_i2c(self.address)
        result = None
        if b:
            logger.debug(f"Received {b} bytes! Status {s}")
            result = [hex(c) for c in d]
            logger.debug(f"Callback Response: {result}")
            if self.is_checksum_valid(result) and self.is_length_valid(result):
                self.queue.put(result)
        else:
            logger.debug(f"Received number of bytes was {b}")

    def is_checksum_valid(self, b):
        s = 0x80
        for i in b[:-1]:
            s += int(i, 0)
        checksum = 256 - (s % 256)
        if checksum == 256:
            checksum = 0
        if checksum != int(b[-1], 0):
            logger.debug(f"Checksum invalid (0x{checksum:02x} != {b[-1]})")
            return False
        return True

    def is_length_valid(self, b):
        length_in_msg = int(b[4], 0)
        actual_length = len(b) - 6
        if length_in_msg != actual_length:
            logger.debug(f"Length invalid ({length_in_msg} != {actual_length})")
            return False
        return True

    def close(self):
        self.event_callback.cancel()
        self.pi.bsc_i2c(0)
        self.pi.stop()
