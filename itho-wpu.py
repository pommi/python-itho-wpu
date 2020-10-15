#!/usr/bin/env python3

import argparse
import i2c_raw
import logging
import pigpio
import queue
import sys
import threading

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

    args = parser.parse_args()
    return args


class I2CSlave():
    def __init__(self, address):
        self.address = address
        self.pi = pigpio.pi()
        if not self.pi.connected:
            logger.error("not pi.connected")
            return

    def run(self, q):
        try:
            self.pi.bsc_i2c(self.address)
            if self.pi.wait_for_event(pigpio.EVENT_BSC, 5):
                s, b, d = self.pi.bsc_i2c(self.address)
                result = None
                if b:
                    logger.debug(f"Received {b} bytes! Status {s}")
                    result = [hex(c) for c in d]
                    q.put(result)
                else:
                    logger.error(f"Received number of bytes was {b}")
            else:
                logger.error("pi.wait_for_event timed out")
        except(KeyboardInterrupt):
            self.close()
        else:
            self.close()

    def close(self):
        self.pi.bsc_i2c(0)
        self.pi.stop()


class I2CMaster:
    def __init__(self, address, bus):
        self.i = i2c_raw.I2CRaw(address=address, bus=bus)

    def execute_action(self, action):
        actions = {
            "getregelaar": [0x80, 0x90, 0xE0, 0x04, 0x00, 0x8A],
            "getserial": [0x80, 0x90, 0xE1, 0x04, 0x00, 0x89],
            "getdatatype": [0x80, 0xA4, 0x00, 0x04, 0x00, 0x56],
            "getdatalog": [0x80, 0xA4, 0x01, 0x04, 0x00, 0x55],
        }
        logger.info(f"Executing action: {action}")
        self.i.write_i2c_block_data(actions[action])

    def close(self):
        self.i.close()


if __name__ == "__main__":
    args = parse_args()

    if args.loglevel:
        logger.setLevel(args.loglevel.upper())

    q = queue.Queue()

    slave = I2CSlave(address=0x40)
    slave_thread = threading.Thread(target=slave.run, args=[q])
    slave_thread.start()

    master = I2CMaster(address=0x41, bus=1)
    if args.action:
        master.execute_action(args.action)
    master.close()

    result = q.get()
    logger.info(f"Response: {result}")
