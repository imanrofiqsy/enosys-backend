from django.http import HttpResponse, JsonResponse
import json
from django.views.decorators.csrf import csrf_exempt
import logging
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from django.conf import settings

# global buffer (ram)
pm_buffer = {}      # key = "PM1" ... "PM5"  value = dict data
EXPECTED_PM_COUNT = 50

client = InfluxDBClient(
    url=settings.INFLUXDB["url"],
    token=settings.INFLUXDB["token"],
    org=settings.INFLUXDB["org"],
)
write_api = client.write_api(write_options=SYNCHRONOUS)

def dashboard(request):
    return JsonResponse({"status": "ok", "message": "Backend Railway is running"})

@csrf_exempt
def test(request):
    global pm_buffer

    if request.method == "POST":

        clen = request.META.get("CONTENT_LENGTH")
        if not clen:
            return JsonResponse({"ok": False, "no_content_length": True})

        clen = int(clen)
        body_bytes = request.read(clen)

        if len(body_bytes) < clen:
            logging.warning("payload truncated - skip")
            return JsonResponse({"ok": False, "partial": True})

        raw = body_bytes.decode('utf-8', errors='ignore')
        logging.info(f"data: {raw}")

        # decode JSON
        try:
            body = json.loads(raw)
        except Exception:
            logging.warning("json incomplete / cannot decode - skip")
            return JsonResponse({"ok": False, "invalid_json": True})

        # parsing
        dev   = body.get("device")
        meter = body.get("meter")  # string ex: PM1

        if not meter:
            return JsonResponse({"ok": False, "invalid_meter": True})

        # store meter row into buffer
        pm_buffer[meter] = {
            "Power"  : float(body.get("p", body.get("Power", 0))),
            "Voltage": float(body.get("v", body.get("Voltage", 0))),
            "Current": float(body.get("c", body.get("Current", 0))),
            "Kwh"    : float(body.get("k", body.get("Kwh",    0)))
        }

        logging.info(f"buffer now {len(pm_buffer)}/{EXPECTED_PM_COUNT}")

        # kalau belum lengkap 5 meter → skip / ack saja
        if len(pm_buffer) < EXPECTED_PM_COUNT:
            return JsonResponse({"ok": True, "buffered": True})

        # === WRITE KE INFLUX ===
        try:
            point = Point("plc_data").tag("device", dev)

            # flatten
            for meter_name, row in pm_buffer.items():
                point = point.field(f"{meter_name}_Power",   row["Power"])
                point = point.field(f"{meter_name}_Voltage", row["Voltage"])
                point = point.field(f"{meter_name}_Current", row["Current"])
                point = point.field(f"{meter_name}_Kwh",     row["Kwh"])

            write_api.write(
                bucket=settings.INFLUXDB["bucket"],
                org=settings.INFLUXDB["org"],
                record=point
            )

            logging.info("=== 5 PM complete → data pushed to influx ===")

            pm_buffer.clear()   # reset buffer

        except Exception as e:
            logging.exception("influx write error")
            return JsonResponse({"ok": False, "error": str(e)}, status=500)

        return JsonResponse({"ok": True, "pushed": True})

    forwarded = request.META.get('HTTP_X_FORWARDED_FOR')
    remote = request.META.get('REMOTE_ADDR')

    return JsonResponse({"ok": True, "forwarded": forwarded, "addr": remote})
