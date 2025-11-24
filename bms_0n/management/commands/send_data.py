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
        client = InfluxDBClient(url=INFLUX["url"], token=INFLUX["token"], org=ORG, timeout=60000)
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
                    try:
                        total_yesterday_kwh = temp[0]["value"] - temp[1]["value"]
                        total_yesterday_kwh = round(total_yesterday_kwh, 3)
                    except:
                        total_yesterday_kwh = 0

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
                    |> range(start: today(), stop: now())          // sesuaikan rentang waktu
                    |> filter(fn: (r) =>
                        r._measurement == "power_meter_data"
                        and r.device == "{dev}"
                        and r._field == "kwh"
                    )
                    |> sort(columns: ["_time"], desc: false)
                    |> limit(n: 1)
                    |> group(columns: ["_field"])
                    |> sum()
                    '''
                        tables_dev = query_api.query(flux_dev)
                        dev_kwh = 0.0
                        temp_dev = []
                        for table in tables_dev:
                            for rec in table.records:
                                try:
                                    temp_dev.append({"value": float(rec.get_value())})
                                except:
                                    pass

                        flux_dev = f'''
                    from(bucket: "{BUCKET}")
                    |> range(start: today(), stop: now())          // sesuaikan rentang waktu
                    |> filter(fn: (r) =>
                        r._measurement == "power_meter_data"
                        and r.device == "{dev}"
                        and r._field == "kwh"
                    )
                    |> sort(columns: ["_time"], desc: true)
                    |> limit(n: 1)
                    |> group(columns: ["_field"])
                    |> sum()
                    '''
                        tables_dev = query_api.query(flux_dev)
                        for table in tables_dev:
                            for rec in table.records:
                                try:
                                    temp_dev.append({"value": float(rec.get_value())})
                                except:
                                    pass
                        dev_kwh = temp_dev[0]["value"] - temp_dev[1]["value"]
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

                    # 11) Room status
                    # ---------------------------------------------------------
                    room_status = []

                    for dev in PM_GRID:
                        room_name = f"Room {dev[-1]}"  # Example: PM3 -> Room 3
                        room_id = int(dev[-1])  # Extract room ID from device name

                        # Current power, voltage, ampere, temperature, kWh
                        flux_room = f'''
                        from(bucket: "{BUCKET}")
                        |> range(start: -1h)
                        |> filter(fn: (r) =>
                            r._measurement == "power_meter_data" and
                            r.device == "{dev}" and
                            (r._field == "power" or r._field == "voltage" or r._field == "ampere" or r._field == "temperature" or r._field == "kwh")
                        )
                        |> last()
                        '''
                        tables_room = query_api.query(flux_room)

                        room_data = {
                            "device": dev,
                            "room_name": room_name,
                            "room_id": room_id,
                            "lights": None,
                            "ac": None,
                            "power": None,
                            "voltage": None,
                            "ampere": None,
                            "temperature": None,
                            "kwh": None,
                            "history": None
                        }

                        for table in tables_room:
                            for rec in table.records:
                                field = rec.get_field()
                                value = rec.get_value()
                                if field in room_data:
                                    room_data[field] = round(float(value), 2)

                        # Lights and AC state
                        flux_state = f'''
                        from(bucket: "{BUCKET}")
                        |> range(start: -2h)
                        |> filter(fn: (r) =>
                            r._measurement == "power_meter_data" and
                            r.device == "{dev}" and (r._field == "ac" or r._field == "lamp")
                        )
                        |> last()
                        '''
                        tables_state = query_api.query(flux_state)

                        for table in tables_state:
                            for rec in table.records:
                                if rec.get_field() == "ac":
                                    room_data["ac"] = bool(rec.get_value())
                                elif rec.get_field() == "lamp":
                                    room_data["lights"] = bool(rec.get_value())

                        # History for power and temperature (hourly for the last 24 hours)
                        flux_history = f'''
                        from(bucket: "{BUCKET}")
                        |> range(start: {datetime.now(timezone(timedelta(hours=7))).date()}T00:00:00Z, stop: now())
                        |> filter(fn: (r) =>
                            r._measurement == "power_meter_data" and
                            r.device == "{dev}" and (r._field == "kwh" or r._field == "temperature" or r._field == "ampere" or r._field == "voltage")
                        )
                        |> aggregateWindow(every: 1h, fn: mean, createEmpty: false)
                        |> keep(columns: ["_time", "_value", "_field"])
                        '''
                        tables_history = query_api.query(flux_history)

                        for table in tables_history:
                            for rec in table.records:
                                field = rec.get_field()
                                time = rec.get_time().strftime("%H:%M")
                                value = round(float(rec.get_value()), 2)
                                if time not in room_data["history"]:
                                    room_data["history"][time] = {"time": time}
                                room_data["history"][time][field] = value

                        room_status.append(room_data)

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

                    room_status_data = safe_json(room_status)

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
                    send("realtime_chart", realtime_chart)
                    send("weekly_chart", weekly_chart)
                    send("overview_room", overview_data)
                    send("system_status", system_status)
                    send("room_status", room_status_data)

                    # flux_yesterday = f'''
                    # import "experimental"
                    # yesterday_start = experimental.subDuration(d: 1d, from: today())
                    # today_start = today()
                    # from(bucket: "{BUCKET}")
                    # |> range(start: today(), stop: now())          // sesuaikan rentang waktu
                    # |> filter(fn: (r) => 
                    #         r._measurement == "power_meter_data" and 
                    #         r._field == "kwh" and
                    #         r.device =~ /^PM[1-7]$/
                    #     )
                    # |> sort(columns: ["_time"], desc: false)
                    # |> limit(n: 1)
                    # '''
                    # tables = query_api.query(flux_yesterday)
                    # temp = []
                    # for table in tables:
                    #     for rec in table.records:
                    #         temp.append({
                    #             "value": round(float(rec.get_value()), 3)
                    #         })

                    # ping_value = safe_json(temp)
                    # send("ping", ping_value)

                except Exception as e:
                    logger.exception("Failed building/sending dashboard payload: %s", e)

                # sleep until next poll
                from time import sleep
                sleep(POLL_INTERVAL)

        except KeyboardInterrupt:
            self.stdout.write(self.style.WARNING("Stopped by user"))
