#!/usr/bin/env python3

import argparse
import logging
import queue
import sys
import time
import os
import json
import db
from collections import namedtuple
from itho_i2c import I2CMaster, I2CSlave

logger = logging.getLogger('stdout')
logger.setLevel(logging.INFO)
stdout_log_handler = logging.StreamHandler(sys.stdout)
stdout_log_handler.setFormatter(logging.Formatter("%(message)s"))
logger.addHandler(stdout_log_handler)


actions = {
    "getnodeid": [0x90, 0xE0],
    "getserial": [0x90, 0xE1],
    "getdatatype": [0xA4, 0x00],
    "getdatalog": [0xA4, 0x01],
}


def parse_args():
    parser = argparse.ArgumentParser(description='Itho WPU i2c master')

    parser.add_argument('--action', nargs='?', required=True,
                        choices=actions.keys(), help="Execute an action")
    parser.add_argument('--loglevel', nargs='?',
                        choices=["debug", "info", "warning", "error", "critical"],
                        help="Loglevel")
    parser.add_argument('--timestamp', action='store_true', help="Show timestamp in output")
    parser.add_argument('--master-only', action='store_true', help="Only run I2C master")
    parser.add_argument('--slave-only', action='store_true', help="Only run I2C slave")
    parser.add_argument('--slave-timeout', nargs='?', type=int, default=60,
                        help="Slave timeout in seconds when --slave-only")
    parser.add_argument('--no-cache', action='store_true',
                        help="Don't use local cache")
    parser.add_argument('--export-to-influxdb', action='store_true',
                        help="Export results to InfluxDB")

    args = parser.parse_args()
    return args


class IthoWPU():
    def __init__(self, master_only, slave_only, slave_timeout, no_cache):
        self.master_only = master_only
        self.slave_only = slave_only
        self.slave_timeout = slave_timeout
        self._q = queue.Queue()
        self.no_cache = no_cache
        self.cache = IthoWPUCache()
        self.nodeid = self.get('getnodeid')
        self.datatype = self.get('getdatatype')
        self.heatpump_db = db.sqlite('heatpump.sqlite')

    def get(self, action):
        if not self.no_cache:
            response = self.cache.get(action.replace('get', ''))
            if response is not None:
                logger.debug(f"Response (from cache): {response}")
                return response

        response = None

        if not self.master_only:
            slave = I2CSlave(address=0x40, queue=self._q)
            slave.set_callback()
            if self.slave_only:
                time.sleep(self.slave_timeout)

        if not self.slave_only:
            master = I2CMaster(address=0x41, bus=1, queue=self._q)
            if action:
                response = master.execute_action(action)
                logger.debug(f"Response: {response}")
            master.close()

        if not self.master_only:
            slave.close()

        self.cache.set(action.replace('get', ''), response)

        return response

    def get_listversion_from_nodeid(self):
        if self.nodeid is None:
            return
        return int(self.nodeid[10], 0)

    def get_datalog_structure(self):
        listversion = self.get_listversion_from_nodeid()
        datalabel_version = self.heatpump_db.execute(
            f"SELECT datalabel FROM versiebeheer WHERE version = {listversion}")[0]['datalabel']
        if datalabel_version is None or not type(datalabel_version) == int:
            logger.error(f"Datalabel not found in database for version {listversion}")
            return None
        datalabel = self.heatpump_db.execute(
            f"SELECT name, title, tooltip, unit FROM datalabel_v{datalabel_version} order by id")

        if len(self.datatype[5:-1]) != len(datalabel):
            logger.warning(f"Number of datatype items ({len(self.datatype[5:-1])}) is not equal to the number of datalabels ({len(datalabel)}) in the database.")

        Field = namedtuple('Field', 'index type label description')

        datalog = []
        index = 0
        for dl, dt in zip(datalabel, self.datatype[5:-1]):
            description = dl['title'].title()
            if dl['unit'] is not None:
                description = f"{description} ({dl['unit']})"
            description = f"{description} ({dl['name'].lower()})"
            datalog.append(Field(index, int(dt, 0), dl['name'].lower(), description))

            if dt in ['0x0', '0xc']:
                index = index + 1
            elif dt in ['0x10', '0x12', '0x92']:
                index = index + 2
            elif dt in ['0x20']:
                index = index + 4
            else:
                logger.error(f"Unknown data type for label {dl['name']}: {dt}")
                return datalog
        return datalog


class IthoWPUCache:
    def __init__(self):
        self._cache_file = "itho-wpu-cache.json"
        self._cache_data = {
            'nodeid': None,
            'serial': None,
            'datatype': None,
            'schema_version': '1',
        }
        self._read_cache()

    def _read_cache(self):
        if not os.path.exists(self._cache_file):
            logger.debug(f"Not loading cache file: {self._cache_file} does not exist")
            return
        with open(self._cache_file) as cache_file:
            cache_data = json.load(cache_file)
            logger.debug(f"Loading local cache: {json.dumps(cache_data)}")
            for key in ['nodeid', 'serial', 'datatype']:
                if key in cache_data:
                    self._cache_data[key] = cache_data[key]

    def _write_cache(self):
        with open(self._cache_file, 'w') as cache_file:
            logger.debug(f"Writing to local cache: {json.dumps(self._cache_data)}")
            json.dump(self._cache_data, cache_file)

    def get(self, action):
        if action not in ['nodeid', 'serial', 'datatype']:
            logger.debug(f"Cache for '{action}' is not supported")
            return None
        logger.debug(f"Reading '{action}' from local cache")
        if self._cache_data[action] is None:
            logger.debug(f"Action '{action}' is not present in local cache")
        return self._cache_data[action]

    def set(self, action, value):
        if action not in ['nodeid', 'serial', 'datatype']:
            logger.debug(f"Cache for '{action}' is not supported")
            return None
        logger.debug(f"Writing '{action}' to local cache: {value}")
        self._cache_data[action] = value
        self._write_cache()


def is_messageclass_valid(action, response):
    if int(response[1], 0) != actions[action][0] and int(response[2], 0) != actions[action][1]:
        logger.error(f"Response MessageClass != {actions[action][0]} {actions[action][1]} "
                     f"({action}), but {response[1]} {response[2]}")
        return False
    return True


def process_response(action, response, args, wpu):
    if int(response[3], 0) != 0x01:
        logger.error(f"Response MessageType != 0x01 (response), but {response[3]}")
        return
    if not is_messageclass_valid(action, response):
        return

    if action == "getdatalog":
        measurements = process_datalog(response, wpu)
        if args.export_to_influxdb:
            from itho_export import export_to_influxdb
            export_to_influxdb(action, measurements)
    elif action == "getnodeid":
        process_nodeid(response)
    elif action == "getserial":
        process_serial(response)


def process_nodeid(response):
    hardware_info = {
        0: {
            "name": "HCCP",
            "type": {
                13: "WPU",
                15: "Autotemp",
            }
        }
    }
    manufacturergroup = ((int(response[5], 0) << 8) + int(response[6], 0))
    manufacturer = hardware_info[int(response[7], 0)]["name"]
    hardwaretype = hardware_info[int(response[7], 0)]["type"][int(response[8], 0)]
    productversion = int(response[9], 0)
    listversion = int(response[10], 0)

    logger.info(f"ManufacturerGroup: {manufacturergroup}, Manufacturer: {manufacturer}, "
                f"HardwareType: {hardwaretype}, ProductVersion: {productversion}, "
                f"ListVersion: {listversion}")


def process_serial(response):
    serial = (int(response[5], 0) << 16) + (int(response[6], 0) << 8) + int(response[7], 0)
    logger.info(f"Serial: {serial}")


def process_datalog(response, wpu):
    datalog = wpu.get_datalog_structure()
    message = response[5:]
    measurements = {}
    for d in datalog:
        if d.type == 0x0 or d.type == 0xc:
            m = message[d.index:d.index+1]
            num = int(m[0], 0)
        elif d.type == 0x10:
            m = message[d.index:d.index+2]
            num = ((int(m[0], 0) << 8) + int(m[1], 0))
        elif d.type == 0x12:
            m = message[d.index:d.index+2]
            num = round((int(m[0], 0) << 8) + int(m[1], 0) / 100, 2)
        elif d.type == 0x92:
            m = message[d.index:d.index+2]
            num = ((int(m[0], 0) << 8) + int(m[1], 0))
            if num >= 32768:
                num -= 65536
            num = round(num / 100, 2)
        elif d.type == 0x20:
            m = message[d.index:d.index+4]
            num = ((int(m[0], 0) << 24) + (int(m[1], 0) << 16) + (int(m[2], 0) << 8) + int(m[3], 0))
        else:
            logger.error(f"Unknown message type for datalog {d.name}: {d.type}")
        logger.info(f"{d.description}: {num}")
        measurements[d.label] = num
    return measurements


if __name__ == "__main__":
    args = parse_args()

    if args.loglevel:
        logger.setLevel(args.loglevel.upper())

    if args.timestamp:
        stdout_log_handler.setFormatter(
            logging.Formatter("%(asctime)-15s %(levelname)s: %(message)s")
        )

    wpu = IthoWPU(args.master_only, args.slave_only, args.slave_timeout, args.no_cache)
    response = wpu.get(args.action)
    if response is not None:
        process_response(args.action, response, args, wpu)
