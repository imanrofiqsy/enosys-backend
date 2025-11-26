# bms_0n/utils/send_data_single.py
import asyncio
import logging
import math
from datetime import datetime, timezone, timedelta

from django.conf import settings

from influxdb_client import InfluxDBClient
from asgiref.sync import async_to_sync, sync_to_async
from channels.layers import get_channel_layer


logger = logging.getLogger(__name__)

# ---- GLOBALS (diisi oleh initialize()) ----
BUCKET = None
COST_PER_KWH = None
PM_SOLAR = None
PM_LIST = None
PM_GRID = None
ONLINE_THRESHOLD_SECONDS = None

client = None
query_api = None
channel_layer = None
group_name = None


# -------------------------
# helpers untuk akses settings (tidak dieksekusi saat import)
# -------------------------
def get_influx_settings():
    return getattr(settings, "INFLUXDB", {})


def get_influx_client():
    influx = get_influx_settings()
    return InfluxDBClient(
        url=influx.get("url", ""),
        token=influx.get("token", ""),
        org=influx.get("org", "")
    )


def get_bucket():
    influx = get_influx_settings()
    return influx.get("bucket", "")


def get_cost_per_kwh():
    return float(getattr(settings, "COST_PER_KWH", 1444))


def get_online_threshold_seconds():
    return int(getattr(settings, "DASHBOARD_ONLINE_THRESHOLD", 90))


# -------------------------
# initialize: dipanggil sebelum melakukan query / kirim
# -------------------------
def initialize():
    global client, query_api, BUCKET, channel_layer, group_name
    global PM_SOLAR, PM_LIST, PM_GRID, COST_PER_KWH, ONLINE_THRESHOLD_SECONDS

    # Influx client & query api
    client = get_influx_client()
    try:
        query_api = client.query_api()
    except Exception as e:
        # jika client gagal dibuat, set query_api ke None dan log error
        logger.exception("Failed creating Influx query_api: %s", e)
        query_api = None

    BUCKET = get_bucket()

    # Channel layer (pastikan Django sudah siap)
    try:
        channel_layer = get_channel_layer()
    except Exception as e:
        logger.exception("Failed getting channel layer: %s", e)
        channel_layer = None

    group_name = getattr(settings, "DASHBOARD_CHANNEL_GROUP", "dashboard_group")

    COST_PER_KWH = get_cost_per_kwh()
    ONLINE_THRESHOLD_SECONDS = get_online_threshold_seconds()

    PM_LIST = [f"PM{i}" for i in range(1, 9)]
    PM_GRID = [f"PM{i}" for i in range(1, 8)]
    PM_SOLAR = "PM8"


# -------------------------
# safety wrapper untuk query
# -------------------------
def safe_query(flux):
    """Jalankan query Influx, kembalikan list(table) atau [] saat error."""
    if query_api is None:
        logger.warning("safe_query called but query_api is None")
        return []
    try:
        tables = query_api.query(flux)
        return tables
    except Exception as e:
        logger.exception("Influx query failed: %s", e)
        return []
    
async def send_async(group_name: str, topic: str, data):
    """
    Kirim pesan ke channel group dalam lingkungan ASGI (async).
    Dipakai dari consumers.py atau async tasks.
    """
    if channel_layer is None:
        logger.warning("channel_layer is None (async), skip send.")
        return

    try:
        await channel_layer.group_send(
            group_name,
            {
                "type": topic,
                "data": data,
            }
        )
    except Exception as e:
        logger.exception("Async group_send failed: %s", e)


def send_sync(group_name: str, topic: str, data):
    """
    Kirim pesan dari lingkungan non-async (Django command, cron).
    """
    if channel_layer is None:
        logger.warning("channel_layer is None (sync), skip send.")
        return

    try:
        async_to_sync(channel_layer.group_send)(
            group_name,
            {
                "type": topic,
                "data": data,
            }
        )
    except Exception as e:
        logger.exception("Sync group_send failed: %s", e)


def send(group_name: str, topic: str, data):
    """
    AUTO — pilih async atau sync berdasarkan kondisi.
    """
    try:
        loop = asyncio.get_running_loop()
        # Jika ada event loop aktif → gunakan async
        return asyncio.create_task(send_async(group_name, topic, data))
    except RuntimeError:
        # Jika TIDAK ada event loop → aman pakai sync
        return send_sync(group_name, topic, data)


# -------------------------
# fungsi-fungsi utama (sebagian besar sama logikanya dengan versi sebelumnya)
# -------------------------
def calculate_total_today_kwh():
    global BUCKET
    flux_today = f'''
    first = 
    from(bucket: "{BUCKET}")
        |> range(start: today(), stop: now())
        |> filter(fn: (r) => 
            r._measurement == "power_meter_data" and 
            r._field == "kwh" and
            r.device =~ /^PM[1-7]$/
        )
        |> group(columns:["device"])
        |> sort(columns: ["_time"], desc: false)
        |> limit(n:1)

    last = 
    from(bucket: "{BUCKET}")
        |> range(start: today(), stop: now())
        |> filter(fn: (r) => 
            r._measurement == "power_meter_data" and 
            r._field == "kwh" and
            r.device =~ /^PM[1-7]$/
        )
        |> group(columns:["device"])
        |> sort(columns: ["_time"], desc: true)
        |> limit(n:1)

    union(tables: [first, last])
    '''

    tables = safe_query(flux_today)

    first_values = {}
    last_values = {}

    for table in tables:
        for rec in table.records:
            try:
                dev = rec.values.get("device")
                val = float(rec.get_value() or 0.0)
                time = rec.get_time()
            except Exception:
                continue

            if dev is None:
                continue

            if dev not in first_values or time < first_values[dev]["time"]:
                first_values[dev] = {"time": time, "value": val}

            if dev not in last_values or time > last_values[dev]["time"]:
                last_values[dev] = {"time": time, "value": val}

    total_first = sum(v["value"] for v in first_values.values()) if first_values else 0.0
    total_last = sum(v["value"] for v in last_values.values()) if last_values else 0.0

    total_today_kwh = round(max(0.0, total_last - total_first), 3)
    return total_today_kwh


def calculate_total_yesterday_kwh():
    global BUCKET
    flux_today = f'''
    import "experimental"
    yesterday_start = experimental.subDuration(d: 1d, from: today())
    today_start = today()
    first = 
    from(bucket: "{BUCKET}")
        |> range(start: yesterday_start, stop: today_start)
        |> filter(fn: (r) => 
            r._measurement == "power_meter_data" and 
            r._field == "kwh" and
            r.device =~ /^PM[1-7]$/
        )
        |> group(columns:["device"])
        |> sort(columns: ["_time"], desc: false)
        |> limit(n:1)

    last = 
    from(bucket: "{BUCKET}")
        |> range(start: yesterday_start, stop: today_start)
        |> filter(fn: (r) => 
            r._measurement == "power_meter_data" and 
            r._field == "kwh" and
            r.device =~ /^PM[1-7]$/
        )
        |> group(columns:["device"])
        |> sort(columns: ["_time"], desc: true)
        |> limit(n:1)

    union(tables: [first, last])
    '''

    tables = safe_query(flux_today)

    first_values = {}
    last_values = {}

    for table in tables:
        for rec in table.records:
            try:
                dev = rec.values.get("device")
                val = float(rec.get_value() or 0.0)
                time = rec.get_time()
            except Exception:
                continue

            if dev is None:
                continue

            if dev not in first_values or time < first_values[dev]["time"]:
                first_values[dev] = {"time": time, "value": val}

            if dev not in last_values or time > last_values[dev]["time"]:
                last_values[dev] = {"time": time, "value": val}

    total_first = sum(v["value"] for v in first_values.values()) if first_values else 0.0
    total_last = sum(v["value"] for v in last_values.values()) if last_values else 0.0

    total_yesterday_kwh = round(max(0.0, total_last - total_first), 3)
    return total_yesterday_kwh


def pct_change(new, old):
    if old == 0:
        if new == 0:
            return 0.0
        return float("inf") if new > 0 else float("-inf")
    return round(((new - old) / old) * 100.0, 2)


def alarm_counts():
    global BUCKET
    flux_alarms_active = f'''
    from(bucket: "{BUCKET}")
    |> range(start: -7d)
    |> filter(fn: (r) =>
        r._measurement == "alarm_data_new" and
        r._field == "status" and r._value == "active"
    )
    |> count()
    '''

    active_alarm_count = 0
    for table in safe_query(flux_alarms_active):
        for rec in table.records:
            try:
                active_alarm_count += int(rec.get_value() or 0)
            except Exception:
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

    high_alarm_count = 0
    for table in safe_query(flux_alarms_high):
        for rec in table.records:
            try:
                high_alarm_count += int(rec.get_value() or 0)
            except Exception:
                pass

    return active_alarm_count, high_alarm_count


def calculate_solar_today_kwh():
    global BUCKET, PM_SOLAR
    flux_today = f'''
    first = 
    from(bucket: "{BUCKET}")
        |> range(start: today(), stop: now())
        |> filter(fn: (r) => 
            r._measurement == "power_meter_data" and 
            r._field == "kwh" and
            r.device == "{PM_SOLAR}"
        )
        |> sort(columns: ["_time"], desc: false)
        |> limit(n:1)

    last = 
    from(bucket: "{BUCKET}")
        |> range(start: today(), stop: now())
        |> filter(fn: (r) => 
            r._measurement == "power_meter_data" and 
            r._field == "kwh" and
            r.device == "{PM_SOLAR}"
        )
        |> sort(columns: ["_time"], desc: true)
        |> limit(n:1)

    union(tables: [first, last])
    '''

    tables = safe_query(flux_today)

    first_values = {}
    last_values = {}

    for table in tables:
        for rec in table.records:
            try:
                dev = rec.values.get("device")
                val = float(rec.get_value() or 0.0)
                time = rec.get_time()
            except Exception:
                continue

            if dev is None:
                continue

            if dev not in first_values or time < first_values[dev]["time"]:
                first_values[dev] = {"time": time, "value": val}

            if dev not in last_values or time > last_values[dev]["time"]:
                last_values[dev] = {"time": time, "value": val}

    total_first = sum(v["value"] for v in first_values.values()) if first_values else 0.0
    total_last = sum(v["value"] for v in last_values.values()) if last_values else 0.0

    solar_today_kwh = round(max(0.0, total_last - total_first), 3)
    return solar_today_kwh


def pct_solar_share(solar_kwh, total_kwh):
    try:
        return round((solar_kwh / total_kwh) * 100.0, 2) if total_kwh > 0 else 0.0
    except Exception:
        return 0.0


def realtime_chart():
    global BUCKET
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

    tables = safe_query(flux_realtime_24)

    realtime_points = []
    for table in tables:
        for rec in table.records:
            try:
                realtime_points.append({
                    "time": rec.get_time().isoformat(),
                    "value": round(float(rec.get_value() or 0.0), 3)
                })
            except Exception:
                pass

    # keep last 24 points (1 per hour in previous version — leaving original behaviour)
    realtime_points = realtime_points[-24:]
    return realtime_points


def weekly_pln_vs_solar():
    global BUCKET, PM_SOLAR
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

    tables_pln = safe_query(flux_weekly_pln)
    tables_solar = safe_query(flux_weekly_solar)

    def records_to_daily_energy(tables):
        data_per_device = {}

        for table in tables:
            for rec in table.records:
                try:
                    device = rec.values.get("device")
                    t = rec.get_time().astimezone(timezone.utc).date()
                    val = float(rec.get_value() or 0.0)
                except Exception:
                    continue

                if device not in data_per_device:
                    data_per_device[device] = {}

                if t not in data_per_device[device]:
                    data_per_device[device][t] = []

                data_per_device[device][t].append(val)

        day_energy = {}

        for device, days in data_per_device.items():
            for day, values in days.items():
                energy = max(values) - min(values)
                day_str = str(day)
                day_energy[day_str] = day_energy.get(day_str, 0) + energy

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
    return weekly_pln, weekly_solar


def room_overview():
    global BUCKET, PM_GRID
    overview = []

    if not PM_GRID:
        return overview

    for dev in PM_GRID:
        # Power today per device (first & last)
        flux_dev_first = f'''
        from(bucket: "{BUCKET}")
        |> range(start: today(), stop: now())
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
        tables_dev_first = safe_query(flux_dev_first)
        temp_dev = []
        for table in tables_dev_first:
            for rec in table.records:
                try:
                    temp_dev.append({"value": float(rec.get_value() or 0.0)})
                except Exception:
                    pass

        flux_dev_last = f'''
        from(bucket: "{BUCKET}")
        |> range(start: today(), stop: now())
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
        tables_dev_last = safe_query(flux_dev_last)
        for table in tables_dev_last:
            for rec in table.records:
                try:
                    temp_dev.append({"value": float(rec.get_value() or 0.0)})
                except Exception:
                    pass

        dev_kwh = 0.0
        try:
            if len(temp_dev) >= 2:
                dev_kwh = round(max(0.0, temp_dev[1]["value"] - temp_dev[0]["value"]), 3)
        except Exception:
            dev_kwh = 0.0

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
        tables_state = safe_query(flux_state)
        ac_state = None
        lamp_state = None

        for table in tables_state:
            for rec in table.records:
                try:
                    if rec.get_field() == "ac":
                        ac_state = bool(rec.get_value())
                    elif rec.get_field() == "lamp":
                        lamp_state = bool(rec.get_value())
                except Exception:
                    pass

        overview.append({
            "device": dev,
            "power_kwh": dev_kwh,
            "ac": ac_state,
            "lamp": lamp_state
        })

    return overview


def system_online_status():
    global ONLINE_THRESHOLD_SECONDS
    flux_last = f'''
    from(bucket: "{BUCKET}")
    |> range(start: -10m)
    |> filter(fn: (r) => r._measurement == "power_meter_data")
    |> last()
    '''

    tables = safe_query(flux_last)
    last_time = None
    for table in tables:
        for rec in table.records:
            try:
                last_time = rec.get_time()
            except Exception:
                pass

    system_online = False
    now = datetime.now(timezone.utc)
    if last_time and ONLINE_THRESHOLD_SECONDS is not None:
        try:
            delta = now - last_time.astimezone(timezone.utc)
            system_online = delta.total_seconds() <= ONLINE_THRESHOLD_SECONDS
        except Exception:
            system_online = False
    return system_online


def room_status():
    global PM_GRID
    room_status = []

    if not PM_GRID:
        return room_status

    for dev in PM_GRID:
        room_name = f"Room {dev[-1]}"
        try:
            room_id = int(dev[-1])
        except Exception:
            room_id = 0

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
        tables_room = safe_query(flux_room)

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
            "history": []
        }

        for table in tables_room:
            for rec in table.records:
                try:
                    field = rec.get_field()
                    value = rec.get_value()
                    if field in room_data and value is not None:
                        room_data[field] = round(float(value), 2)
                except Exception:
                    pass

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
        tables_state = safe_query(flux_state)
        for table in tables_state:
            for rec in table.records:
                try:
                    if rec.get_field() == "ac":
                        room_data["ac"] = bool(rec.get_value())
                    elif rec.get_field() == "lamp":
                        room_data["lights"] = bool(rec.get_value())
                except Exception:
                    pass

        # History (hourly last 24h)
        flux_history = f'''
        from(bucket: "{BUCKET}")
        |> range(start: -24h)
        |> filter(fn: (r) =>
            r._measurement == "power_meter_data" and
            r.device == "{dev}" and 
            (r._field == "kwh" or r._field == "temperature" or 
            r._field == "ampere" or r._field == "voltage")
        )
        |> aggregateWindow(every: 1h, fn: mean, createEmpty: false)
        |> keep(columns: ["_time", "_value", "_field"])
        '''
        tables_history = safe_query(flux_history)

        for table in tables_history:
            for rec in table.records:
                try:
                    field = rec.get_field()
                    ts = rec.get_time().astimezone(timezone(timedelta(hours=7)))
                    time_full = ts.strftime("%Y-%m-%d %H:%M")
                    time_hour = ts.strftime("%H:%M")
                    value = round(float(rec.get_value() or 0.0), 2)

                    entry = next((item for item in room_data["history"] if item["time_full"] == time_full), None)
                    if not entry:
                        entry = {"time_full": time_full, "time": time_hour}
                        room_data["history"].append(entry)

                    entry[field] = value
                except Exception:
                    pass

        room_data["history"].sort(key=lambda x: x["time_full"])
        room_status.append(room_data)

    return room_status


def build_dashboard_payload():
    """
    Public entry point.
    Panggil initialize() terlebih dahulu (initialize akan aman dipanggil berkali-kali).
    """
    try:
        initialize()
    except Exception as e:
        # initialize seharusnya aman, tapi log kalau ada masalah
        logger.exception("initialize() failed: %s", e)

    now = datetime.now(timezone.utc)

    # ambil data
    try:
        total_today_kwh = calculate_total_today_kwh()
    except Exception:
        logger.exception("calculate_total_today_kwh failed")
        total_today_kwh = 0.0

    try:
        total_yesterday_kwh = calculate_total_yesterday_kwh()
    except Exception:
        logger.exception("calculate_total_yesterday_kwh failed")
        total_yesterday_kwh = 0.0

    total_today_cost = round(total_today_kwh * (COST_PER_KWH or get_cost_per_kwh()), 2)
    total_yesterday_cost = round(total_yesterday_kwh * (COST_PER_KWH or get_cost_per_kwh()), 2)
    pct_power = pct_change(total_today_kwh, total_yesterday_kwh)
    pct_cost = pct_change(total_today_cost, total_yesterday_cost)

    try:
        active_alarm_count, high_alarm_count = alarm_counts()
    except Exception:
        logger.exception("alarm_counts failed")
        active_alarm_count, high_alarm_count = 0, 0

    try:
        solar_today_kwh = calculate_solar_today_kwh()
    except Exception:
        logger.exception("calculate_solar_today_kwh failed")
        solar_today_kwh = 0.0

    total_load = solar_today_kwh + total_today_kwh
    solar_share_pct = pct_solar_share(solar_today_kwh, total_load)

    try:
        realtime_points = realtime_chart()
    except Exception:
        logger.exception("realtime_chart failed")
        realtime_points = []

    try:
        weekly_pln, weekly_solar = weekly_pln_vs_solar()
    except Exception:
        logger.exception("weekly_pln_vs_solar failed")
        weekly_pln, weekly_solar = [], []

    try:
        overview = room_overview()
    except Exception:
        logger.exception("room_overview failed")
        overview = []

    try:
        system_online = system_online_status()
    except Exception:
        logger.exception("system_online_status failed")
        system_online = False

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
        elif isinstance(data, datetime):
            return data.isoformat()
        return data

    power_summary = safe_json({
        "timestamp": now.isoformat(),
        "total_today_kwh": total_today_kwh,
        "pct_change_power_vs_yesterday": pct_power,
        "total_today_cost": total_today_cost,
        "pct_change_cost_vs_yesterday": pct_cost,
    })

    alarms_status = safe_json({
        "active_alarms": active_alarm_count,
        "high_priority_alarms": high_alarm_count,
    })

    solar_data = safe_json({
        "solar_today_kwh": solar_today_kwh,
        "solar_share_pct": solar_share_pct,
        "pln_today_kwh": total_today_kwh,
    })

    realtime_chart_data = safe_json(realtime_points)

    weekly_chart = safe_json({
        "pln": weekly_pln,
        "solar": weekly_solar,
    })

    overview_data = safe_json(overview)

    system_status = safe_json({
        "system_online": system_online,
    })

    send("dashboard_group","power_summary", power_summary)
    send("dashboard_group","alarms_status", alarms_status)
    send("dashboard_group","solar_data", solar_data)
    send("dashboard_group","realtime_chart", realtime_chart_data)
    send("dashboard_group","weekly_chart", weekly_chart)
    send("dashboard_group","overview_room", overview_data)
    send("dashboard_group","system_status", system_status)

def build_room_status_payload():
    """
    Public entry point.
    Panggil initialize() terlebih dahulu (initialize akan aman dipanggil berkali-kali).
    """
    try:
        initialize()
    except Exception as e:
        # initialize seharusnya aman, tapi log kalau ada masalah
        logger.exception("initialize() failed: %s", e)

    now = datetime.now(timezone.utc)

    # ambil data
    try:
        room_status_data = room_status()
    except Exception:
        logger.exception("room_status failed")
        room_status_data = []

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
        elif isinstance(data, datetime):
            return data.isoformat()
        return data

    room_status_json = safe_json(room_status_data)

    send("dashboard_group","room_status", room_status_json)