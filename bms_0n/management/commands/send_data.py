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
COST_PER_KWH = float(getattr(settings, "COST_PER_KWH", 0.15))

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
                    pm_flux_array = "[" + ", ".join([f'"{pm}"' for pm in PM_LIST]) + "]"

                    flux_today = f'''
                    from(bucket: "{BUCKET}")
                    |> range(start: today())
                    |> filter(fn: (r) => r._measurement == "power_meter_data" and r._field == "kwh")
                    |> filter(fn: (r) => contains(value: r.device, set: {pm_flux_array}))
                    |> aggregateWindow(every: 1h, fn: mean, createEmpty: false)
                    |> integral(unit: 1h)
                    |> sum()
                    '''

                    tables = query_api.query(flux_today)
                    total_today_kwh = 0.0
                    for table in tables:
                        for rec in table.records:
                            try:
                                total_today_kwh += float(rec.get_value())
                            except:
                                pass
                    total_today_kwh = round(total_today_kwh, 3)

                    # ---------------------------------------------------------
                    # 2) Total power usage yesterday (kWh)
                    # ---------------------------------------------------------
                    flux_yesterday = f'''
                    import "date"
                    import "timezone"
                    option location = timezone.location(name: "Asia/Jakarta")

                    startYesterday = date.sub(from: date.truncate(t: now(), unit: 1d), d: 1d)
                    startTwoDaysAgo = date.sub(from: date.truncate(t: now(), unit: 1d), d: 2d)

                    data = from(bucket: "{BUCKET}")
                    |> range(start: startTwoDaysAgo, stop: startYesterday)
                    |> filter(fn: (r) =>
                        r._measurement == "power_meter_data" and
                        r._field == "kwh" and
                        contains(value: r.device, set: {pm_flux_array})
                    )

                    firstVals = data
                    |> first()

                    lastVals = data
                    |> last()

                    join(
                    tables: {{f: firstVals, l: lastVals}},
                    on: ["device"]
                    )
                    |> map(fn: (r) => ({{
                        device: r.device,
                        diff: r._value_l - r._value_f
                    }}))
                    |> sum(column: "diff")

                    '''

                    tables = query_api.query(flux_yesterday)
                    total_yesterday_kwh = 0.0
                    for table in tables:
                        for rec in table.records:
                            try:
                                total_yesterday_kwh += float(rec.get_value())
                            except:
                                pass
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
                    flux_solar_today = f'''
                    from(bucket: "{BUCKET}")
                    |> range(start: today())
                    |> filter(fn: (r) =>
                        r._measurement == "power_meter_data"
                        and r.device == "{PM_SOLAR}"
                        and r._field == "kwh"
                    )
                    |> aggregateWindow(every: 1h, fn: mean, createEmpty: false)
                    |> integral(unit: 1h)
                    |> sum()
                    '''

                    tables = query_api.query(flux_solar_today)
                    solar_today_kwh = 0.0
                    for table in tables:
                        for rec in table.records:
                            try:
                                solar_today_kwh += float(rec.get_value())
                            except:
                                pass
                    solar_today_kwh = round(solar_today_kwh, 3)

                    # ---------------------------------------------------------
                    # PLN today (PM1..PM7) for solar share
                    # ---------------------------------------------------------
                    flux_pln_today = f'''
                    from(bucket: "{BUCKET}")
                    |> range(start: today())
                    |> filter(fn: (r) =>
                        r._measurement == "power_meter_data"
                        and r._field == "kwh"
                        and r.device =~ /PM[1-7]/
                    )
                    |> aggregateWindow(every: 1h, fn: mean, createEmpty: false)
                    |> integral(unit: 1h)
                    |> sum()
                    '''

                    tables = query_api.query(flux_pln_today)
                    pln_today_kwh = 0.0
                    for table in tables:
                        for rec in table.records:
                            try:
                                pln_today_kwh += float(rec.get_value())
                            except:
                                pass
                    pln_today_kwh = round(pln_today_kwh, 3)

                    total_load = solar_today_kwh + pln_today_kwh
                    solar_share_pct = round((solar_today_kwh / total_load) * 100.0, 2) if total_load > 0 else 0.0

                    # ---------------------------------------------------------
                    # 7) Realtime chart: Minute-by-minute difference total power (PM1..PM7)
                    # ---------------------------------------------------------
                    flux_realtime_24 = f'''
                    from(bucket: "{BUCKET}")
                    |> range(start: -24h)
                    |> filter(fn: (r) =>
                        r._measurement == "power_meter_data" and
                        r._field == "kwh" and
                        r.device =~ /PM[1-7]/
                    )
                    |> aggregateWindow(every: 1m, fn: last, createEmpty: false)
                    |> difference(columns: ["_value"])
                    |> group(columns: ["_time"])
                    |> sum(column: "_value")
                    '''

                    tables = query_api.query(flux_realtime_24)

                    realtime_points = []
                    for table in tables:
                        for rec in table.records:
                            try:
                                realtime_points.append({
                                    "time": rec.get_time().isoformat(),
                                    "value": round(float(rec.get_value()), 3)
                                })
                            except:
                                pass

                    # keep last 24 hours only
                    realtime_points = realtime_points[-24:]

                    # -------------------------
                    # 8) PLN vs Solar per day for last 7 days
                    # -------------------------
                    # Simpler approach: query per-day sums for pln and solar separately
                    flux_weekly_pln = f'''
                    from(bucket: "{BUCKET}")
                    |> range(start: -7d)
                    |> filter(fn: (r) =>
                        r._measurement == "power_meter_data" and
                        r._field == "kwh" and
                        r.device =~ /PM[1-7]/
                    )
                    |> keep(columns: ["_time", "_value", "device"])
                    '''

                    flux_weekly_solar = f'''
                    from(bucket: "{BUCKET}")
                    |> range(start: -7d)
                    |> filter(fn: (r) =>
                        r._measurement == "power_meter_data" and
                        r._field == "kwh" and
                        r.device == "{PM_SOLAR}"
                    )
                    |> keep(columns: ["_time", "_value", "device"])
                    '''

                    tables_pln = query_api.query(flux_weekly_pln)
                    tables_solar = query_api.query(flux_weekly_solar)

                    def records_to_daily_energy(tables):
                        # Simpan per device
                        data_per_device = {}

                        for table in tables:
                            for rec in table.records:
                                device = rec.values.get("device")
                                t = rec.get_time().astimezone(timezone.utc).date()
                                val = float(rec.get_value() or 0)

                                if device not in data_per_device:
                                    data_per_device[device] = {}

                                # simpan setiap sample berdasarkan tanggal
                                if t not in data_per_device[device]:
                                    data_per_device[device][t] = []

                                data_per_device[device][t].append(val)

                        # hitung energi per hari
                        day_energy = {}

                        for device, days in data_per_device.items():
                            for day, values in days.items():
                                # kWh cumulative → daily energy = last - first
                                energy = max(values) - min(values)

                                day_str = str(day)
                                day_energy[day_str] = day_energy.get(day_str, 0) + energy

                        # format output 7 hari terakhir
                        today = datetime.now(timezone.utc).date()
                        out = []

                        for offset in reversed(range(7)):
                            day = today - timedelta(days=offset)
                            day_str = str(day)
                            out.append({
                                "date": day_str,
                                "value": round(day_energy.get(day_str, 0.0), 3)
                            })

                        return out

                    weekly_pln = records_to_daily_energy(tables_pln)
                    weekly_solar = records_to_daily_energy(tables_solar)

                    # ---------------------------------------------------------
                    # 9) Overview per room: power, AC, lamp
                    # ---------------------------------------------------------
                    overview = []

                    for dev in PM_GRID:
                        # Power today per device
                        flux_dev = f'''
                    from(bucket: "{BUCKET}")
                    |> range(start: today())
                    |> filter(fn: (r) =>
                        r._measurement == "power_meter_data"
                        and r.device == "{dev}"
                        and r._field == "kwh"
                    )
                    |> aggregateWindow(every: 1h, fn: mean, createEmpty: false)
                    |> integral(unit: 1h)
                    |> sum()
                    '''
                        tables_dev = query_api.query(flux_dev)
                        dev_kwh = 0.0
                        for table in tables_dev:
                            for rec in table.records:
                                try:
                                    dev_kwh += float(rec.get_value())
                                except:
                                    pass
                        dev_kwh = round(dev_kwh, 3)

                        # AC & lamp state
                        flux_state = f'''
                    from(bucket: "{BUCKET}")
                    |> range(start: -2h)
                    |> filter(fn: (r) =>
                        r._measurement == "power_meter_data"
                        and r.device == "{dev}"
                        and (r._field == "ac" or r._field == "lamp")
                    )
                    |> last()
                    '''
                        tables_state = query_api.query(flux_state)
                        ac_state = None
                        lamp_state = None

                        for table in tables_state:
                            for rec in table.records:
                                if rec.get_field() == "ac":
                                    ac_state = bool(rec.get_value())
                                elif rec.get_field() == "lamp":
                                    lamp_state = bool(rec.get_value())

                        overview.append({
                            "device": dev,
                            "power_kwh": dev_kwh,
                            "ac": ac_state,
                            "lamp": lamp_state
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
                        "pln_today_kwh": pln_today_kwh,
                    })

                    # --- 4) Real-time Power Chart ---
                    realtime_chart = safe_json(realtime_points)

                    # --- 5) Weekly PLN vs Solar ---
                    weekly_chart = safe_json({
                        "pln": weekly_pln,
                        "solar": weekly_solar,
                    })

                    # --- 6) Overview by Room ---
                    overview_data = safe_json(overview)

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

                    flux_query = f'''
                    from(bucket: "{BUCKET}")
                    |> range(start: -1d)          // sesuaikan rentang waktu
                    |> filter(fn: (r) => 
                            r._measurement == "new_pm_data" and 
                            r._field == "kwh" and
                            r.device == "PM1"
                        )
                    '''
                    tables = query_api.query(flux_query)
                    dummy = []
                    for table in tables:
                        for rec in table.records:
                            dummy.append({
                                "time": rec.get_time().isoformat(),
                                "value": round(float(rec.get_value()), 3)
                            })

                    send("ping", {"data": dummy})

                    send("power_summary", power_summary)
                    send("alarms_status", alarms_status)
                    send("solar_data", solar_data)
                    send("realtime_chart", realtime_chart)
                    send("weekly_chart", weekly_chart)
                    send("overview_room", overview_data)
                    send("system_status", system_status)
                    # send("ping", ping)
                    

                except Exception as e:
                    logger.exception("Failed building/sending dashboard payload: %s", e)

                # sleep until next poll
                from time import sleep
                sleep(POLL_INTERVAL)

        except KeyboardInterrupt:
            self.stdout.write(self.style.WARNING("Stopped by user"))
