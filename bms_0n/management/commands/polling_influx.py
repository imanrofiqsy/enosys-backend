import time
import random
import logging
from datetime import datetime, timezone

from django.core.management.base import BaseCommand
from django.conf import settings

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

logger = logging.getLogger(__name__)

POLL_INTERVAL = int(getattr(settings, "INFLUX_POLL_INTERVAL", 5))

class Command(BaseCommand):
    help = "Generate dummy Power Meter & Alarm data to InfluxDB"

    def load_last_kwh(self, client, bucket, device):
        """
        Load last KWH from InfluxDB.
        If no data exists → return 1000.0
        """
        query_api = client.query_api()

        flux = f'''
        from(bucket: "{bucket}")
            |> range(start: -30d)
            |> filter(fn: (r) =>
                r._measurement == "power_meter_data" and
                r._field == "kwh" and
                r.device == "{device}"
            )
            |> last()
        '''

        tables = query_api.query(flux)

        for table in tables:
            for rec in table.records:
                try:
                    return float(rec.get_value())
                except:
                    pass

        # default jika tidak ada data
        return 1000.0

    def handle(self, *args, **options):
        # init influx client
        client = InfluxDBClient(
            url=settings.INFLUXDB["url"],
            token=settings.INFLUXDB["token"],
            org=settings.INFLUXDB["org"],
        )
        write_api = client.write_api(write_options=SYNCHRONOUS)
        query_api = client.query_api()
        bucket = settings.INFLUXDB["bucket"]

        self.stdout.write(self.style.SUCCESS("Starting Dummy Data Generator (with load state from InfluxDB)"))

        # ============================================
        # LOAD INITIAL STATE KWH DARI INFLUX
        # ============================================
        kwh_counters = {}
        for i in range(1, 9):
            device = f"PM{i}"
            last_value = self.load_last_kwh(client, bucket, device)
            kwh_counters[device] = round(last_value, 3)

        logger.info(f"Loaded KWH state from InfluxDB: {kwh_counters}")

        try:
            while True:
                timestamp = datetime.now(timezone.utc)

                power_points = []
                for i in range(1, 9):
                    device = f"PM{i}"

                    # Increase cumulative KWH
                    delta = random.uniform(0.01, 0.10)
                    kwh_counters[device] += delta
                    kwh = round(kwh_counters[device], 3)

                    voltage = round(random.uniform(210, 240), 2)
                    ampere = round(random.uniform(5, 25), 2)
                    temperature = round(random.uniform(30, 80), 2)
                    ac = random.randint(0, 1)
                    lamp = random.randint(0, 1)

                    point = (
                        Point("power_meter_data")
                        .tag("device", device)
                        .field("kwh", kwh)
                        .field("voltage", voltage)
                        .field("ampere", ampere)
                        .field("temperature", temperature)
                        .field("ac", ac)
                        .field("lamp", lamp)
                        .time(timestamp)
                    )
                    power_points.append(point)

                # Alarm random 30%
                alarm_points = []
                if random.random() < 0.3:
                    sources = [f"PM{i}" for i in range(1, 9)]
                    severities = ["Low", "Medium", "High", "Critical"]
                    messages = [
                        "Over temperature",
                        "Voltage too low",
                        "Current spike",
                        "Communication lost",
                        "Power drop detected",
                    ]
                    actions = ["Investigate", "Restart", "Ignore", "Shutdown"]

                    alarm_point = (
                        Point("alarm_data_new")
                        .field("timestamp", timestamp.isoformat())
                        .field("source", random.choice(sources))
                        .field("severity", random.choice(severities))
                        .field("message", random.choice(messages))
                        .field("status", random.choice(["active", "resolved"]))
                        .field("action", random.choice(actions))
                        .time(timestamp)
                    )
                    alarm_points.append(alarm_point)

                # Write to Influx
                all_points = power_points + alarm_points
                write_api.write(bucket=bucket, record=all_points)

                logger.info(
                    f"Written {len(power_points)} PM data + {len(alarm_points)} alarms at {timestamp}"
                )

                time.sleep(POLL_INTERVAL)

        except KeyboardInterrupt:
            self.stdout.write(self.style.WARNING("Dummy data generator stopped by user"))
        except Exception as e:
            logger.exception(f"Error while writing dummy data: {e}")
