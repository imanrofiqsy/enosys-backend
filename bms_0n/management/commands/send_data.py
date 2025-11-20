import logging
logging.basicConfig(level=logging.INFO)
import math
from datetime import datetime, timezone, timedelta

from django.conf import settings
from django.core.management.base import BaseCommand

from influxdb_client import InfluxDBClient
from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer

logger = logging.getLogger(__name__)

POLL_INTERVAL = int(getattr(settings, "INFLUX_POLL_INTERVAL", 5))
INFLUX = settings.INFLUXDB
BUCKET = INFLUX["bucket"]
ORG = INFLUX["org"]

# cost per kWh (currency per kWh) — set in settings or default
COST_PER_KWH = float(getattr(settings, "COST_PER_KWH", 1444))

# devices
PM_SOLAR = "PM8"
PM_LIST = [f"PM{i}" for i in range(1, 9)]
PM_GRID = [f"PM{i}" for i in range(1, 8)]  # 1..7 -> main PLN per asumsi

# online freshness threshold (seconds)
ONLINE_THRESHOLD_SECONDS = int(getattr(settings, "DASHBOARD_ONLINE_THRESHOLD", 90))


class Command(BaseCommand):
    help = "Fetch metrics from InfluxDB and push aggregated payload to channels group 'dashboard_group'"

    def handle(self, *args, **options):
        client = InfluxDBClient(url=INFLUX["url"], token=INFLUX["token"], org=ORG)
        query_api = client.query_api()
        channel_layer = get_channel_layer()
        group_name = "dashboard_group"

        self.stdout.write(self.style.SUCCESS("Starting Influx->WebSocket publisher"))

        try:
            while True:
                try:
                    now = datetime.now(timezone.utc)
                    # ---------------------------------------------------------
                    # Realtime power usage
                    # ---------------------------------------------------------

                    flux_realtime_chart = f'''
                    from(bucket: "{BUCKET}")
                    |> range(start: today(), stop: now())
                    |> filter(fn: (r) =>
                        r._measurement == "power_meter_data" and
                        r._field == "kwh" and
                        r.device =~ /^PM[1-7]$/
                    )
                    |> keep(columns: ["_time", "_value", "device"])
                    |> group(columns: ["device"])
                    |> fill(usePrevious: true)
                    |> aggregateWindow(every: 1h, fn: last, createEmpty: true)
                    |> difference(nonNegative: true)
                    |> group(columns: ["_time"])
                    |> sum()
                    |> sort(columns: ["_time"])
                    '''

                    tables = query_api.query(flux_realtime_chart)
                    realtime_data = []
                    for table in tables:
                        for rec in table.records:
                            value = rec.get_value()
                            if value is None:
                                continue  # skip record yang kosong

                            realtime_data.append({
                                "time": rec.get_time().isoformat(),
                                "value": round(float(value), 3)
                            })

                    # ---------------------------------------------------------
                    # pln vs solar weekly chart
                    # ---------------------------------------------------------

                    flux_pln_weekly = f'''
                    from(bucket: "{BUCKET}")
                    |> range(start: -7d)
                    |> filter(fn: (r) =>
                        r._measurement == "power_meter_data" and
                        r._field == "kwh" and
                        r.device =~ /^PM[1-7]$/
                    )
                    |> group(columns: ["device"])
                    |> fill(usePrevious: true)
                    |> aggregateWindow(every: 1d, fn: last, createEmpty: true)
                    |> fill(usePrevious: true)             // ⭐ Tambahan penting
                    |> difference(nonNegative: true)
                    |> filter(fn: (r) => exists r._value)  // ⭐ Hilangkan hari yang null
                    |> group(columns: ["_time"])
                    |> sum()
                    |> sort(columns: ["_time"])

                    '''
                    tables = query_api.query(flux_pln_weekly)
                    pln_weekly_data = []
                    for table in tables:
                        for rec in table.records:
                            value = rec.get_value()
                            if value is None:
                                continue  # skip record yang kosong

                            pln_weekly_data.append({
                                "time": rec.get_time().isoformat(),
                                "value": round(float(value), 3)
                            })
                    # ---------------------------------------------------------
                    # System online/offline check
                    # ---------------------------------------------------------
                    flux_last = f'''
                    from(bucket: "{BUCKET}")
                    |> range(start: -10m)
                    |> filter(fn: (r) => r._measurement == "power_meter_data")
                    |> last()
                    '''

                    tables = query_api.query(flux_last)
                    last_time = None
                    for table in tables:
                        for rec in table.records:
                            last_time = rec.get_time()

                    system_online = False
                    if last_time:
                        delta = now - last_time.astimezone(timezone.utc)
                        system_online = delta.total_seconds() <= ONLINE_THRESHOLD_SECONDS

                    # -------------------------
                    # Build payload
                    # -------------------------

                    def safe_json(data):
                        def fix_value(v):
                            if isinstance(v, float):
                                if math.isnan(v) or math.isinf(v):
                                    return 0.0
                            return v

                        if isinstance(data, dict):
                            return {k: fix_value(v) for k, v in data.items()}
                        elif isinstance(data, list):
                            return [fix_value(v) for v in data]
                        elif isinstance(data, datetime.datetime):
                            return data.isoformat()
                        return data

                    # --- 7) System Online Status ---
                    system_status = safe_json({
                        "system_online": system_online,
                    })

                    def send(topic, data):
                        async_to_sync(channel_layer.group_send)(
                            group_name,
                            {
                                "type": topic,
                                "data": data,
                            },
                        )

                    send("system_status", system_status)
                    send("realtime_chart", safe_json(realtime_data))
                    send("ping", safe_json(pln_weekly_data))

                except Exception as e:
                    logger.exception("Failed building/sending dashboard payload: %s", e)

                # sleep until next poll
                from time import sleep
                sleep(POLL_INTERVAL)

        except KeyboardInterrupt:
            self.stdout.write(self.style.WARNING("Stopped by user"))
