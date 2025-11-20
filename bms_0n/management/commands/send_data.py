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
                    # 1) Total power usage today (kWh)
                    # ---------------------------------------------------------
                    flux_today = f'''
                    from(bucket: "{BUCKET}")
                    |> range(start: today(), stop: now())          // sesuaikan rentang waktu
                    |> filter(fn: (r) => 
                            r._measurement == "power_meter_data" and 
                            r._field == "kwh" and
                            r.device =~ /^PM[1-7]$/
                        )
                    |> sort(columns: ["_time"], desc: false)
                    |> limit(n: 1)
                    |> group(columns: ["_field"])
                    |> sum()
                    '''
                    tables = query_api.query(flux_today)
                    temp = []
                    for table in tables:
                        for rec in table.records:
                            temp.append({
                                "value": round(float(rec.get_value()), 3)
                            })

                    flux_today = f'''
                    from(bucket: "{BUCKET}")
                    |> range(start: today(), stop: now())          // sesuaikan rentang waktu
                    |> filter(fn: (r) => 
                            r._measurement == "power_meter_data" and 
                            r._field == "kwh" and
                            r.device =~ /^PM[1-7]$/
                        )
                    |> sort(columns: ["_time"], desc: true)
                    |> limit(n: 1)
                    |> group(columns: ["_field"])
                    |> sum()
                    '''

                    tables = query_api.query(flux_today)
                    for table in tables:
                        for rec in table.records:
                            temp.append({
                                "value": round(float(rec.get_value()), 3)
                            })
                    total_today_kwh = temp[0]["value"] - temp[1]["value"]
                    total_today_kwh = round(total_today_kwh, 3)

                    # ---------------------------------------------------------
                    # 2) Total power usage yesterday (kWh)
                    # ---------------------------------------------------------
                    flux_yesterday = f'''
                    import "experimental"
                    yesterday_start = experimental.subDuration(d: 1d, from: today())
                    today_start = today()
                    from(bucket: "{BUCKET}")
                    |> range(start: yesterday_start, stop: today_start)          // sesuaikan rentang waktu
                    |> filter(fn: (r) => 
                            r._measurement == "power_meter_data" and 
                            r._field == "kwh" and
                            r.device =~ /^PM[1-7]$/
                        )
                    |> sort(columns: ["_time"], desc: false)
                    |> limit(n: 1)
                    |> group(columns: ["_field"])
                    |> sum()
                    '''
                    tables = query_api.query(flux_yesterday)
                    temp = []
                    for table in tables:
                        for rec in table.records:
                            temp.append({
                                "value": round(float(rec.get_value()), 3)
                            })

                    flux_yesterday = f'''
                    import "experimental"
                    yesterday_start = experimental.subDuration(d: 1d, from: today())
                    today_start = today()
                    from(bucket: "{BUCKET}")
                    |> range(start: yesterday_start, stop: today_start)          // sesuaikan rentang waktu
                    |> filter(fn: (r) => 
                            r._measurement == "power_meter_data" and 
                            r._field == "kwh" and
                            r.device =~ /^PM[1-7]$/
                        )
                    |> sort(columns: ["_time"], desc: true)
                    |> limit(n: 1)
                    |> group(columns: ["_field"])
                    |> sum()
                    '''

                    tables = query_api.query(flux_yesterday)
                    for table in tables:
                        for rec in table.records:
                            temp.append({
                                "value": round(float(rec.get_value()), 3)
                            })
                    total_yesterday_kwh = temp[0]["value"] - temp[1]["value"]
                    total_yesterday_kwh = round(total_yesterday_kwh, 3)

                    # ---------------------------------------------------------
                    # 3) Cost today & yesterday
                    # ---------------------------------------------------------
                    total_today_cost = round(total_today_kwh * COST_PER_KWH, 2)
                    total_yesterday_cost = round(total_yesterday_kwh * COST_PER_KWH, 2)

                    # ---------------------------------------------------------
                    # 4) Percentage change helper
                    # ---------------------------------------------------------
                    def pct_change(new, old):
                        if old == 0:
                            if new == 0:
                                return 0.0
                            return float("inf") if new > 0 else float("-inf")
                        return round(((new - old) / old) * 100.0, 2)

                    pct_power = pct_change(total_today_kwh, total_yesterday_kwh)
                    pct_cost  = pct_change(total_today_cost, total_yesterday_cost)

                    # ---------------------------------------------------------
                    # 5) Alarms (active count & high priority)
                    # ---------------------------------------------------------
                    flux_alarms_active = f'''
                    from(bucket: "{BUCKET}")
                    |> range(start: -7d)
                    |> filter(fn: (r) =>
                        r._measurement == "alarm_data_new" and
                        r._field == "status" and r._value == "active"
                    )
                    |> count()
                    '''

                    tables = query_api.query(flux_alarms_active)
                    active_alarm_count = 0
                    for table in tables:
                        for rec in table.records:
                            try:
                                active_alarm_count += int(rec.get_value())
                            except:
                                pass

                    flux_alarms_high = f'''
                    from(bucket: "{BUCKET}")
                    |> range(start: -7d)
                    |> filter(fn: (r) =>
                        r._measurement == "alarm_data_new"
                        and r._field == "severity"
                        and (r._value == "High" or r._value == "Critical")
                    )
                    |> count()
                    '''

                    tables = query_api.query(flux_alarms_high)
                    high_alarm_count = 0
                    for table in tables:
                        for rec in table.records:
                            try:
                                high_alarm_count += int(rec.get_value())
                            except:
                                pass

                    # ---------------------------------------------------------
                    # 6) Total solar output today (PM8)
                    # ---------------------------------------------------------
                    flux_solar = f'''
                    from(bucket: "{BUCKET}")
                    |> range(start: today(), stop: now())          // sesuaikan rentang waktu
                    |> filter(fn: (r) => 
                            r._measurement == "power_meter_data" and 
                            r._field == "kwh" and
                            r.device == "{PM_SOLAR}"
                        )
                    |> sort(columns: ["_time"], desc: false)
                    |> limit(n: 1)
                    |> group(columns: ["_field"])
                    |> sum()
                    '''
                    tables = query_api.query(flux_solar)
                    temp = []
                    for table in tables:
                        for rec in table.records:
                            temp.append({
                                "value": round(float(rec.get_value()), 3)
                            })

                    flux_solar = f'''
                    from(bucket: "{BUCKET}")
                    |> range(start: today(), stop: now())          // sesuaikan rentang waktu
                    |> filter(fn: (r) => 
                            r._measurement == "power_meter_data" and 
                            r._field == "kwh" and
                            r.device == "{PM_SOLAR}"
                        )
                    |> sort(columns: ["_time"], desc: true)
                    |> limit(n: 1)
                    |> group(columns: ["_field"])
                    |> sum()
                    '''

                    tables = query_api.query(flux_solar)
                    for table in tables:
                        for rec in table.records:
                            temp.append({
                                "value": round(float(rec.get_value()), 3)
                            })
                    solar_today_kwh = temp[0]["value"] - temp[1]["value"]
                    solar_today_kwh = round(solar_today_kwh, 3)

                    # ---------------------------------------------------------
                    # PLN today (PM1..PM7) for solar share
                    # ---------------------------------------------------------

                    total_load = solar_today_kwh + total_today_kwh
                    solar_share_pct = round((solar_today_kwh / total_load) * 100.0, 2) if total_load > 0 else 0.0

                    # ---------------------------------------------------------
                    # 7) Realtime power usage
                    # ---------------------------------------------------------

                    flux_realtime_chart = f'''
                    from(bucket: "{BUCKET}")
                    |> range(start: today(), stop: now())
                    |> filter(fn: (r) =>
                        r._measurement == "power_meter_data" and
                        r._field == "kwh" and
                        r.device =~ /^PM[1-7]$/
                    )
                    |> aggregateWindow(every: 1h, fn: last, createEmpty: false)
                    |> difference(nonNegative: true)
                    |> group(columns: ["_time"])
                    |> sum()
                    |> filter(fn: (r) => exists r._value)
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
                    # 10) System online/offline check
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

                    # --- 1) Power Summary ---
                    power_summary = safe_json({
                        "timestamp": now.isoformat(),
                        "total_today_kwh": total_today_kwh,
                        "pct_change_power_vs_yesterday": pct_power,
                        "total_today_cost": total_today_cost,
                        "pct_change_cost_vs_yesterday": pct_cost,
                    })

                    # --- 2) Alarm Summary ---
                    alarms_status = safe_json({
                        "active_alarms": active_alarm_count,
                        "high_priority_alarms": high_alarm_count,
                    })

                    # --- 3) Solar Info ---
                    solar_data = safe_json({
                        "solar_today_kwh": solar_today_kwh,
                        "solar_share_pct": solar_share_pct,
                        "pln_today_kwh": total_today_kwh,
                    })

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

 
                    send("power_summary", power_summary)
                    send("alarms_status", alarms_status)
                    send("solar_data", solar_data)
                    send("system_status", system_status)
                    send("realtime_chart", safe_json(realtime_data))

                except Exception as e:
                    logger.exception("Failed building/sending dashboard payload: %s", e)

                # sleep until next poll
                from time import sleep
                sleep(POLL_INTERVAL)

        except KeyboardInterrupt:
            self.stdout.write(self.style.WARNING("Stopped by user"))
