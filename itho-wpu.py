#!/usr/bin/env python3

import argparse
import i2c_raw
import logging
import pigpio
import queue
import sys
import time

consolelogformatter = logging.Formatter("%(asctime)-15s %(levelname)s: %(message)s")
logger = logging.getLogger('stdout')
logger.setLevel(logging.INFO)
stdout_log_handler = logging.StreamHandler(sys.stdout)
stdout_log_handler.setFormatter(consolelogformatter)
logger.addHandler(stdout_log_handler)


def parse_args():
    parser = argparse.ArgumentParser(description='Itho WPU i2c master')

    actions = [
        "getregelaar",
        "getserial",
        "getdatatype",
        "getdatalog",
    ]
    parser.add_argument('--action', nargs='?', required=True,
                        choices=actions, help="Execute an action")
    parser.add_argument('--loglevel', nargs='?',
                        choices=["debug", "info", "warning", "error", "critical"],
                        help="Loglevel")
    parser.add_argument('--master-only', action='store_true', help="Only run I2C master")
    parser.add_argument('--slave-only', action='store_true', help="Only run I2C slave")
    parser.add_argument('--slave-timeout', nargs='?', type=int, default=60,
                        help="Slave timeout in seconds when --slave-only")

    args = parser.parse_args()
    return args


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
            if self.is_checksum_valid(result):
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

    def close(self):
        self.event_callback.cancel()
        self.pi.bsc_i2c(0)
        self.pi.stop()


class I2CMaster:
    def __init__(self, address, bus, queue):
        self.i = i2c_raw.I2CRaw(address=address, bus=bus)
        self.queue = queue

    def execute_action(self, action):
        actions = {
            "getregelaar": [0x80, 0x90, 0xE0, 0x04, 0x00, 0x8A],
            "getserial": [0x80, 0x90, 0xE1, 0x04, 0x00, 0x89],
            "getdatatype": [0x80, 0xA4, 0x00, 0x04, 0x00, 0x56],
            "getdatalog": [0x80, 0xA4, 0x01, 0x04, 0x00, 0x55],
        }
        result = None
        for i in range(0, 20):
            logger.debug(f"Executing action: {action}")
            self.i.write_i2c_block_data(actions[action])
            time.sleep(0.21)
            logger.debug("Queue size: {}".format(self.queue.qsize()))
            if self.queue.qsize() > 0:
                result = self.queue.get()
                break

        if result is None:
            logger.error(f"No valid result in 20 requests")
        return result

    def close(self):
        self.i.close()


if __name__ == "__main__":
    args = parse_args()

    if args.loglevel:
        logger.setLevel(args.loglevel.upper())

    q = queue.Queue()

    if not args.master_only:
        slave = I2CSlave(address=0x40, queue=q)
        slave.set_callback()
        if args.slave_only:
            time.sleep(args.slave_timeout)

    if not args.slave_only:
        master = I2CMaster(address=0x41, bus=1, queue=q)
        if args.action:
            result = master.execute_action(args.action)
            logger.info(f"Response: {result}")
        master.close()

    if not args.master_only:
        slave.close()
