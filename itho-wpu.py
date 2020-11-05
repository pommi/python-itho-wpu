#!/usr/bin/env python3

import argparse
import i2c_raw
import logging
import pigpio
import queue
import sys
import time
from collections import namedtuple

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


def process_response(action, response):
    if action == "getdatalog" and int(response[1], 0) == 0xA4 and int(response[2], 0) == 0x01:
        measurements = process_datalog(response)


def process_datalog(response):
    # 0 = Byte
    # 1 = UnsignedInt
    # 2 = SignedIntDec2
    Field = namedtuple('Field', 'index type label description')
    datalog = [
        Field(0, 2, "t_out", "Buitentemperatuur"),
        Field(2, 2, "t_boiltop", "Boiler laag"),
        Field(4, 2, "t_boildwn", "Boiler hoog"),
        Field(6, 2, "t_evap", "Verdamper temperatuur"),
        Field(8, 2, "t_suct", "Zuiggas temperatuur"),
        Field(10, 2, "t_disc", "Persgas temperatuur"),
        Field(12, 2, "t_cond", "Vloeistof temperatuur"),
        Field(14, 2, "t_source_r", "Naar bron"),
        Field(16, 2, "t_source_s", "Van bron"),
        Field(18, 2, "t_ch_supp", "CV aanvoer"),
        Field(20, 2, "t_ch_ret", "CV retour"),
        Field(22, 2, "p_sens", "Druksensor (Bar)"),
        Field(24, 2, "i_tr1", "Stroom trafo 1 (A)"),
        Field(26, 2, "i_tr2", "Stroom trafo 2 (A)"),
        Field(34, 1, "in_flow", "Flow sensor bron (l/h)"),
        Field(37, 0, "out_ch", "Snelheid cv pomp (%)"),
        Field(38, 0, "out_src", "Snelheid bron pomp (%)"),
        Field(39, 0, "out_dhw", "Snelheid boiler pomp (%)"),
        Field(44, 0, "out_c1", "Compressor aan/uit"),
        Field(45, 0, "out_ele", "Elektrisch element aan/uit"),
        Field(46, 0, "out_trickle", "Trickle heating aan/uit"),
        Field(47, 0, "out_fault", "Fout aanwezig (0=J, 1=N)"),
        Field(48, 0, "out_fc", "Vrijkoelen actief (0=uit, 1=aan)"),
        Field(51, 2, "ot_room", "Kamertemperatuur"),
        Field(55, 0, "ot_mod", "Warmtevraag (%)"),
        Field(56, 0, "state", "State (0=init,1=uit,2=CV,3=boiler,4=vrijkoel,5=ontluchten)"),
        Field(57, 0, "sub_state", "Substatus (255=geen)"),
        Field(67, 0, "fault_reported", "Fout gevonden (foutcode)"),
        Field(92, 1, "tr_fc", "Vrijkoelen interval (sec)"),
    ]
    message = response[5:]
    measurements = {}
    for d in datalog:
        if d.type == 0:
            m = message[d.index:d.index+1]
            num = int(m[0], 0)
        elif d.type == 1:
            m = message[d.index:d.index+2]
            num = ((int(m[0], 0) << 8) + int(m[1], 0))
        elif d.type == 2:
            m = message[d.index:d.index+2]
            num = round(((int(m[0], 0) << 8) + int(m[1], 0)) / 100.0, 2)
        logger.info(f"{d.description}: {num}")
        measurements[d.label] = num
    return measurements


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
            logger.debug(f"Response: {result}")
        master.close()

        if result is not None:
            process_response(args.action, result)

    if not args.master_only:
        slave.close()
