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

    def handle(self, *args, **options):
        # init client (sync)
        client = InfluxDBClient(
            url=settings.INFLUXDB["url"],
            token=settings.INFLUXDB["token"],
            org=settings.INFLUXDB["org"],
        )
        write_api = client.write_api(write_options=SYNCHRONOUS)

        bucket = settings.INFLUXDB["bucket"]

        self.stdout.write(self.style.SUCCESS("Starting Dummy Data Generator for InfluxDB"))

        try:
            while True:
                timestamp = datetime.now(timezone.utc)

                # === 1) Generate Power Meter Dummy Data ===
                power_points = []
                for i in range(1, 9):  # PM1 sampai PM8
                    kwh = round(random.uniform(1000, 2000), 3)
                    voltage = round(random.uniform(210, 240), 2)
                    ampere = round(random.uniform(5, 25), 2)
                    temperature = round(random.uniform(30, 80), 2)

                    point = (
                        Point("plc_data")
                        .tag("device", f"PM{i}")
                        .field("kwh", kwh)
                        .field("voltage", voltage)
                        .field("ampere", ampere)
                        .field("temperature", temperature)
                        .time(timestamp)
                    )
                    power_points.append(point)

                # === 2) Generate Alarm Dummy Data ===
                alarm_points = []
                # Probabilitas muncul alarm
                if random.random() < 0.3:  # 30% chance
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
                        Point("alarm_data")
                        .field("timestamp", timestamp.isoformat())
                        .field("source", random.choice(sources))
                        .field("severity", random.choice(severities))
                        .field("message", random.choice(messages))
                        .field("status", random.choice(["active", "resolved"]))
                        .field("action", random.choice(actions))
                        .time(timestamp)
                    )
                    alarm_points.append(alarm_point)

                # === 3) Tulis ke Influx ===
                all_points = power_points + alarm_points
                write_api.write(bucket=bucket, record=all_points)

                logger.info(f"Written {len(power_points)} power data + {len(alarm_points)} alarms at {timestamp}")
                time.sleep(POLL_INTERVAL)

        except KeyboardInterrupt:
            self.stdout.write(self.style.WARNING("Dummy data generator stopped by user"))
        except Exception as e:
            logger.exception(f"Error while writing dummy data: {e}")
