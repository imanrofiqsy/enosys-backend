import logging
import json
import math
from datetime import datetime, timezone, timedelta
from time import sleep

from django.conf import settings
from django.core.management.base import BaseCommand

from influxdb_client import InfluxDBClient
from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

POLL_INTERVAL = int(getattr(settings, "INFLUX_POLL_INTERVAL", 5))
INFLUX = settings.INFLUXDB
BUCKET = INFLUX["bucket"]
ORG = INFLUX["org"]

# cost per kWh (currency per kWh) — set in settings or default
COST_PER_KWH = float(getattr(settings, "COST_PER_KWH", 1444))

# devices
PM_SOLAR = "PM8"
PM_GRID = [f"PM{i}" for i in range(1, 8)]  # 1..7 -> main PLN per asumsi
PM_ALL_REGEX = "/^PM[1-7]$/"  # used inside flux text where regex literal expected

# online freshness threshold (seconds)
ONLINE_THRESHOLD_SECONDS = int(getattr(settings, "DASHBOARD_ONLINE_THRESHOLD", 90))

# maximum bytes to send to channel in one payload (safety - Upstash limit ~10MB)
# we keep low margin (9_000_000 bytes)
MAX_PAYLOAD_BYTES = 9_000_000

# cap realtime points (to limit payload)
MAX_REALTIME_POINTS = int(getattr(settings, "MAX_REALTIME_POINTS", 1440))  # default 24h @1m = 1440


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

                    # ---------------------------
                    # UTIL: helper to run flux and parse table records
                    # ---------------------------
                    def run_flux(flux, timeout_ms=20000):
                        """Run flux query and return list of records (rows)."""
                        try:
                            tables = query_api.query(flux, org=ORG, # keep org explicit
                                                     # set timeout moderately high for heavier queries
                                                     # Note: influx client expects timeout in seconds usually via client config;
                                                     # here we rely on default but keep function param for future use
                                                     )
                        except Exception as e:
                            logger.exception("Flux query failed: %s", e)
                            return []
                        records = []
                        for table in tables:
                            for rec in table.records:
                                records.append(rec)
                        return records

                    # ---------------------------
                    # 1) Today: first & last per PM1..PM7 (single query)
                    # ---------------------------
                    flux_first_last_today = f'''
import "experimental"
yesterday_start = experimental.subDuration(d: 1d, from: today())  // not used but keeps compatibility
from(bucket: "{BUCKET}")
  |> range(start: today(), stop: now())
  |> filter(fn: (r) =>
      r._measurement == "power_meter_data" and
      r._field == "kwh" and
      r.device =~ /^PM[1-7]$/
  )
  |> group(columns: ["device"])
  |> keep(columns: ["_time", "_value", "device"])
  |> pivot(rowKey:["_time"], columnKey: ["device"], valueColumn: "_value") // not to be used, kept for readability
  // We will instead compute first() and last() via union below
'''

                    # Simpler: use union(first,last)
                    flux_first_last_today = f'''
from(bucket: "{BUCKET}")
  |> range(start: today(), stop: now())
  |> filter(fn: (r) =>
      r._measurement == "power_meter_data" and
      r._field == "kwh" and
      r.device =~ /^PM[1-7]$/
  )
  |> group(columns: ["device"])
  |> keep(columns: ["_time", "_value", "device"])
  |> first()
  |> set(key: "pos", value: "first")
  |> union(
      tables: [
        from(bucket: "{BUCKET}")
          |> range(start: today(), stop: now())
          |> filter(fn: (r) =>
              r._measurement == "power_meter_data" and
              r._field == "kwh" and
              r.device =~ /^PM[1-7]$/
          )
          |> group(columns: ["device"])
          |> keep(columns: ["_time", "_value", "device"])
          |> last()
          |> set(key: "pos", value: "last")
      ]
  )
'''

                    records = run_flux(flux_first_last_today)

                    # collect first/last by device
                    device_vals = {dev: {"first": None, "last": None} for dev in PM_GRID}
                    for rec in records:
                        try:
                            dev = rec.values.get("device")
                            pos = rec.values.get("pos")
                            val = rec.get_value()
                            if dev in device_vals:
                                if pos == "first":
                                    device_vals[dev]["first"] = float(val)
                                elif pos == "last":
                                    device_vals[dev]["last"] = float(val)
                        except Exception:
                            # skip malformed rec
                            pass

                    # compute per-device usage and totals
                    per_device_usage = {}
                    total_today_kwh = 0.0
                    for dev, vals in device_vals.items():
                        f = vals["first"]
                        l = vals["last"]
                        if f is None or l is None:
                            usage = 0.0
                        else:
                            usage = max(0.0, l - f)  # avoid negative due to resets
                        usage = round(usage, 3)
                        per_device_usage[dev] = usage
                        total_today_kwh += usage

                    total_today_kwh = round(total_today_kwh, 3)

                    # ---------------------------
                    # 2) Yesterday total per device (first/last in yesterday range)
                    # ---------------------------
                    flux_first_last_yesterday = f'''
import "experimental"
yesterday_start = experimental.subDuration(d: 1d, from: today())
today_start = today()

from(bucket: "{BUCKET}")
  |> range(start: yesterday_start, stop: today_start)
  |> filter(fn: (r) =>
      r._measurement == "power_meter_data" and
      r._field == "kwh" and
      r.device =~ /^PM[1-7]$/
  )
  |> group(columns: ["device"])
  |> keep(columns: ["_time", "_value", "device"])
  |> first()
  |> set(key: "pos", value: "first")
  |> union(
    tables: [
      from(bucket: "{BUCKET}")
        |> range(start: yesterday_start, stop: today_start)
        |> filter(fn: (r) =>
            r._measurement == "power_meter_data" and
            r._field == "kwh" and
            r.device =~ /^PM[1-7]$/
        )
        |> group(columns: ["device"])
        |> keep(columns: ["_time", "_value", "device"])
        |> last()
        |> set(key: "pos", value: "last")
    ]
  )
'''
                    records_y = run_flux(flux_first_last_yesterday)
                    device_vals_y = {dev: {"first": None, "last": None} for dev in PM_GRID}
                    for rec in records_y:
                        try:
                            dev = rec.values.get("device")
                            pos = rec.values.get("pos")
                            val = rec.get_value()
                            if dev in device_vals_y:
                                if pos == "first":
                                    device_vals_y[dev]["first"] = float(val)
                                elif pos == "last":
                                    device_vals_y[dev]["last"] = float(val)
                        except Exception:
                            pass

                    per_device_usage_yesterday = {}
                    total_yesterday_kwh = 0.0
                    for dev, vals in device_vals_y.items():
                        f = vals["first"]
                        l = vals["last"]
                        if f is None or l is None:
                            usage = 0.0
                        else:
                            usage = max(0.0, l - f)
                        usage = round(usage, 3)
                        per_device_usage_yesterday[dev] = usage
                        total_yesterday_kwh += usage

                    total_yesterday_kwh = round(total_yesterday_kwh, 3)

                    # ---------------------------
                    # 3) Solar (PM8) today
                    # ---------------------------
                    flux_solar_today = f'''
from(bucket: "{BUCKET}")
  |> range(start: today(), stop: now())
  |> filter(fn: (r) =>
      r._measurement == "power_meter_data" and
      r._field == "kwh" and
      r.device == "{PM_SOLAR}"
  )
  |> group()
  |> keep(columns: ["_time", "_value"])
  |> first()
  |> set(key: "pos", value: "first")
  |> union(
    tables: [
      from(bucket: "{BUCKET}")
        |> range(start: today(), stop: now())
        |> filter(fn: (r) =>
            r._measurement == "power_meter_data" and
            r._field == "kwh" and
            r.device == "{PM_SOLAR}"
        )
        |> group()
        |> keep(columns: ["_time", "_value"])
        |> last()
        |> set(key: "pos", value: "last")
    ]
  )
'''
                    records_solar = run_flux(flux_solar_today)
                    solar_first = solar_last = None
                    for rec in records_solar:
                        pos = rec.values.get("pos")
                        try:
                            if pos == "first":
                                solar_first = float(rec.get_value())
                            elif pos == "last":
                                solar_last = float(rec.get_value())
                        except Exception:
                            pass
                    if solar_first is None or solar_last is None:
                        solar_today_kwh = 0.0
                    else:
                        solar_today_kwh = round(max(0.0, solar_last - solar_first), 3)

                    # ---------------------------
                    # 4) Cost & percentage change
                    # ---------------------------
                    total_today_cost = round(total_today_kwh * COST_PER_KWH, 2)
                    total_yesterday_cost = round(total_yesterday_kwh * COST_PER_KWH, 2)

                    def pct_change(new, old):
                        if old == 0:
                            if new == 0:
                                return 0.0
                            return float("inf") if new > 0 else float("-inf")
                        return round(((new - old) / old) * 100.0, 2)

                    pct_power = pct_change(total_today_kwh, total_yesterday_kwh)
                    pct_cost = pct_change(total_today_cost, total_yesterday_cost)

                    # ---------------------------
                    # 5) Alarms (active count & high priority)
                    # ---------------------------
                    flux_alarms_active = f'''
from(bucket: "{BUCKET}")
  |> range(start: -7d)
  |> filter(fn: (r) =>
      r._measurement == "alarm_data_new" and
      r._field == "status" and
      r._value == "active"
  )
  |> count()
'''
                    active_alarm_count = 0
                    try:
                        recs = run_flux(flux_alarms_active)
                        for rec in recs:
                            try:
                                active_alarm_count += int(rec.get_value())
                            except Exception:
                                pass
                    except Exception:
                        pass

                    flux_alarms_high = f'''
from(bucket: "{BUCKET}")
  |> range(start: -7d)
  |> filter(fn: (r) =>
      r._measurement == "alarm_data_new" and
      r._field == "severity" and
      (r._value == "High" or r._value == "Critical")
  )
  |> count()
'''
                    high_alarm_count = 0
                    try:
                        recs = run_flux(flux_alarms_high)
                        for rec in recs:
                            try:
                                high_alarm_count += int(rec.get_value())
                            except Exception:
                                pass
                    except Exception:
                        pass

                    # ---------------------------
                    # 6) Realtime points (minute-by-minute difference total power (PM1..PM7) for last 24h)
                    # ---------------------------
                    flux_realtime_24 = f'''
from(bucket: "{BUCKET}")
  |> range(start: -24h)
  |> filter(fn: (r) =>
      r._measurement == "power_meter_data" and
      r._field == "kwh" and
      r.device =~ /^PM[1-7]$/
  )
  |> aggregateWindow(every: 1m, fn: last, createEmpty: false)
  |> difference()
  |> group(columns: ["_time"])
  |> sum(column: "_value")
  |> keep(columns: ["_time", "_value"])
'''
                    realtime_points = []
                    try:
                        recs = run_flux(flux_realtime_24)
                        for rec in recs:
                            try:
                                realtime_points.append({
                                    "time": rec.get_time().astimezone(timezone.utc).isoformat(),
                                    "value": round(float(rec.get_value()), 3)
                                })
                            except Exception:
                                pass
                    except Exception:
                        pass

                    # keep only last N points to control payload size
                    if len(realtime_points) > MAX_REALTIME_POINTS:
                        realtime_points = realtime_points[-MAX_REALTIME_POINTS:]

                    # ---------------------------
                    # 7) Weekly PLN vs Solar (per-day totals for last 7 days)
                    # We'll rely on the helper function approach but fetch less data (just keep time+value+device)
                    # ---------------------------
                    flux_weekly_pln = f'''
from(bucket: "{BUCKET}")
  |> range(start: -7d)
  |> filter(fn: (r) =>
      r._measurement == "power_meter_data" and
      r._field == "kwh" and
      r.device =~ /^PM[1-7]$/
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
                    tables_pln = run_flux(flux_weekly_pln)
                    tables_solar = run_flux(flux_weekly_solar)

                    def records_to_daily_energy(records):
                        # records: list of influx record objects
                        data_per_day = {}
                        for rec in records:
                            try:
                                device = rec.values.get("device")
                                t = rec.get_time().astimezone(timezone.utc).date()
                                val = float(rec.get_value() or 0)
                                day_str = str(t)
                                if day_str not in data_per_day:
                                    data_per_day[day_str] = {"min": val, "max": val}
                                else:
                                    if val < data_per_day[day_str]["min"]:
                                        data_per_day[day_str]["min"] = val
                                    if val > data_per_day[day_str]["max"]:
                                        data_per_day[day_str]["max"] = val
                            except Exception:
                                pass
                        # convert to list for last 7 days
                        out = []
                        today_date = datetime.now(timezone.utc).date()
                        for offset in reversed(range(7)):
                            day = today_date - timedelta(days=offset)
                            day_str = str(day)
                            if day_str in data_per_day:
                                energy = max(0.0, data_per_day[day_str]["max"] - data_per_day[day_str]["min"])
                            else:
                                energy = 0.0
                            out.append({"date": day_str, "value": round(energy, 3)})
                        return out

                    weekly_pln = records_to_daily_energy(tables_pln)
                    weekly_solar = records_to_daily_energy(tables_solar)

                    # ---------------------------
                    # 8) Overview per room: power, AC, lamp
                    # We'll reuse earlier computed per_device_usage and query last state/metrics
                    # ---------------------------
                    overview = []
                    for dev in PM_GRID:
                        # dev_kwh already calculated in per_device_usage
                        dev_kwh = per_device_usage.get(dev, 0.0)

                        # AC & lamp state and last metrics (power, voltage, ampere, temp, kwh)
                        flux_room = f'''
from(bucket: "{BUCKET}")
  |> range(start: -1h)
  |> filter(fn: (r) =>
      r._measurement == "power_meter_data" and
      r.device == "{dev}" and
      (r._field == "power" or r._field == "voltage" or r._field == "ampere" or r._field == "temp" or r._field == "kwh" or r._field == "ac" or r._field == "lamp")
  )
  |> last()
'''
                        records_room = run_flux(flux_room)
                        room_data = {
                            "device": dev,
                            "room_name": f"Room {dev[-1]}",
                            "room_id": int(dev[-1]) if dev[-1].isdigit() else None,
                            "lights": None,
                            "ac": None,
                            "power": None,
                            "voltage": None,
                            "ampere": None,
                            "temp": None,
                            "kwh": dev_kwh,
                            "history": {"power": [], "temp": []}
                        }
                        for rec in records_room:
                            field = rec.get_field()
                            value = rec.get_value()
                            try:
                                if field == "ac":
                                    room_data["ac"] = bool(value)
                                elif field == "lamp":
                                    room_data["lights"] = bool(value)
                                elif field in ("power", "voltage", "ampere", "temp", "kwh"):
                                    room_data[field] = round(float(value), 2)
                            except Exception:
                                pass

                        # History for power and temperature (hourly for the last 24 hours)
                        flux_history = f'''
from(bucket: "{BUCKET}")
  |> range(start: -24h)
  |> filter(fn: (r) =>
      r._measurement == "power_meter_data" and
      r.device == "{dev}" and (r._field == "power" or r._field == "temp")
  )
  |> aggregateWindow(every: 1h, fn: mean, createEmpty: false)
  |> keep(columns: ["_time", "_value", "_field"])
'''
                        records_hist = run_flux(flux_history)
                        for rec in records_hist:
                            field = rec.get_field()
                            time_label = rec.get_time().astimezone(timezone.utc).strftime("%H:%M")
                            try:
                                value = round(float(rec.get_value()), 2)
                                if field == "power":
                                    room_data["history"]["power"].append({"time": time_label, "value": value})
                                elif field == "temp":
                                    room_data["history"]["temp"].append({"time": time_label, "value": value})
                            except Exception:
                                pass

                        overview.append(room_data)

                    # ---------------------------
                    # 9) System online/offline check
                    # ---------------------------
                    flux_last = f'''
from(bucket: "{BUCKET}")
  |> range(start: -10m)
  |> filter(fn: (r) => r._measurement == "power_meter_data")
  |> last()
'''
                    last_time = None
                    recs = run_flux(flux_last)
                    for rec in recs:
                        last_time = rec.get_time()
                    system_online = False
                    if last_time:
                        delta = now - last_time.astimezone(timezone.utc)
                        system_online = delta.total_seconds() <= ONLINE_THRESHOLD_SECONDS

                    # ---------------------------
                    # Prepare payloads (safe JSON)
                    # ---------------------------
                    def fix_value(v):
                        # recursively fix values before JSON serialization
                        if v is None:
                            return None
                        if isinstance(v, float):
                            if math.isnan(v) or math.isinf(v):
                                return 0.0
                            return v
                        if isinstance(v, dict):
                            return {k: fix_value(val) for k, val in v.items()}
                        if isinstance(v, list):
                            return [fix_value(x) for x in v]
                        if isinstance(v, datetime):
                            return v.isoformat()
                        return v

                    def safe_json(data):
                        return fix_value(data)

                    power_summary = safe_json({
                        "timestamp": now.isoformat(),
                        "total_today_kwh": total_today_kwh,
                        "pct_change_power_vs_yesterday": pct_power,
                        "total_today_cost": total_today_cost,
                        "pct_change_cost_vs_yesterday": pct_cost,
                        "per_device_today_kwh": per_device_usage,
                    })

                    alarms_status = safe_json({
                        "active_alarms": active_alarm_count,
                        "high_priority_alarms": high_alarm_count,
                    })

                    solar_data = safe_json({
                        "solar_today_kwh": solar_today_kwh,
                        "solar_share_pct": round((solar_today_kwh / (solar_today_kwh + total_today_kwh)) * 100.0, 2) if (solar_today_kwh + total_today_kwh) > 0 else 0.0,
                        "pln_today_kwh": total_today_kwh,
                    })

                    realtime_chart = safe_json(realtime_points)
                    weekly_chart = safe_json({"pln": weekly_pln, "solar": weekly_solar})
                    overview_data = safe_json(overview)
                    system_status = safe_json({"system_online": system_online})

                    # ---------------------------
                    # Safe send helper: avoid sending huge payloads that exceed Redis limits
                    # ---------------------------
                    def send(topic, data):
                        payload = {"type": topic, "data": data}
                        # measure size
                        try:
                            raw = json.dumps(payload, default=str)
                            size = len(raw.encode("utf-8"))
                        except Exception:
                            raw = "{}"
                            size = 2
                        if size > MAX_PAYLOAD_BYTES:
                            # if too big, attempt to reduce by trimming large arrays (realtime_chart, overview history)
                            logger.warning("Payload too large (%d bytes). Attempting to shrink before send: topic=%s", size, topic)
                            # shrink strategies
                            if "realtime" in topic and isinstance(data, list):
                                # send only last chunk
                                small = data[-min(MAX_REALTIME_POINTS // 2, 500):] if data else []
                                to_send = {"type": topic, "data": small}
                                async_to_sync(channel_layer.group_send)(group_name, to_send)
                                return
                            # else fallback: send only summary
                            if isinstance(data, dict):
                                summary = {k: data[k] for k in list(data.keys())[:5]}
                                async_to_sync(channel_layer.group_send)(group_name, {"type": topic, "data": summary})
                                return
                            # final fallback: send empty placeholder
                            async_to_sync(channel_layer.group_send)(group_name, {"type": topic, "data": {}})
                            return
                        # normal send
                        async_to_sync(channel_layer.group_send)(group_name, {"type": topic, "data": data})

                    # ---------------------------
                    # Send the prepared payloads
                    # ---------------------------
                    send("power_summary", power_summary)
                    send("alarms_status", alarms_status)
                    send("solar_data", solar_data)
                    send("realtime_chart", realtime_chart)
                    send("weekly_chart", weekly_chart)
                    send("overview_room", overview_data)
                    send("system_status", system_status)
                    # optionally send room_status or others as needed

                except Exception as e:
                    logger.exception("Failed building/sending dashboard payload: %s", e)

                sleep(POLL_INTERVAL)

        except KeyboardInterrupt:
            self.stdout.write(self.style.WARNING("Stopped by user"))
