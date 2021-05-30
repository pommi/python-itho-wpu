import datetime
import os


def export_to_influxdb(action, measurements):
    from influxdb import InfluxDBClient

    influx_client = InfluxDBClient(
        host=os.getenv("INFLUXDB_HOST", "localhost"),
        port=os.getenv("INFLUXDB_PORT", 8086),
        username=os.getenv("INFLUXDB_USERNAME", "root"),
        password=os.getenv("INFLUXDB_PASSWORD", "root"),
        database=os.getenv("INFLUXDB_DATABASE"),
    )
    json_body = [
        {
            "measurement": action,
            "time": datetime.datetime.utcnow().replace(microsecond=0).isoformat(),
            "fields": measurements,
        }
    ]
    try:
        influx_client.write_points(json_body)
    except Exception as e:
        print("Failed to write to influxdb: ", e)
