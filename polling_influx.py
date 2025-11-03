import time
import json
import logging
from datetime import datetime, timezone

from django.core.management.base import BaseCommand
from django.conf import settings

from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS

from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer

logger = logging.getLogger(__name__)

# Poll interval (detik)
POLL_INTERVAL = int(getattr(settings, "INFLUX_POLL_INTERVAL", 5))


class Command(BaseCommand):
    help = "Poll InfluxDB and push latest metrics to channels group 'dashboard_group'"

    def handle(self, *args, **options):
        # init client (sync)
        client = InfluxDBClient(
            url=settings.INFLUXDB["url"],
            token=settings.INFLUXDB["token"],
            org=settings.INFLUXDB["org"],
        )
        query_api = client.query_api()
        channel_layer = get_channel_layer()
        group_name = "dashboard_group"

        self.stdout.write(self.style.SUCCESS("Starting Influx->WebSocket poller"))
        try:
            while True:
                try:
                    # contoh query: ambil point terakhir per measurement "plc_data"
                    # asumsikan measurement = "plc_data" dan fields PM1_Power, PM2_Power, ...
                    flux = f'''
from(bucket: "{settings.INFLUXDB["bucket"]}")
  |> range(start: -1m)
  |> filter(fn: (r) => r._measurement == "plc_data")
  |> last()
'''
                    tables = query_api.query(flux)
                    # kita akan merapikan hasil menjadi single dict
                    # hasil query bisa berisi banyak series -> kita ambil per _field
                    payload = {
                        "time": datetime.now(timezone.utc).isoformat(),
                        "device": None,
                        "fields": {}
                    }

                    for table in tables:
                        for record in table.records:
                            # record._field, record.get_value(), record.values contain tags
                            field_name = record.get_field()  # mis: "PM1_Power"
                            value = record.get_value()
                            # ambil device tag kalau ada
                            if not payload["device"]:
                                payload["device"] = record.values.get("device")
                            payload["fields"][field_name] = value

                    # Jika tidak ada data, kita kirim minimal info (agar UI tahu koneksi ok)
                    if not payload["fields"]:
                        logger.debug("No recent data from influx")
                        payload["note"] = "no_data"

                    # kirim ke group channels
                    async_to_sync(channel_layer.group_send)(
                        group_name,
                        {
                            "type": "send_dashboard_data",
                            "data": payload
                        }
                    )

                    logger.debug("Sent dashboard payload: %s", payload)

                except Exception as e:
                    logger.exception("Polling failed: %s", e)

                time.sleep(POLL_INTERVAL)

        except KeyboardInterrupt:
            self.stdout.write(self.style.WARNING("Poller stopped by user"))
