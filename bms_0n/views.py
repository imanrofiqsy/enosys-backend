from django.http import HttpResponse, JsonResponse
import json
from django.views.decorators.csrf import csrf_exempt
import logging
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from django.conf import settings

# global buffer (ram)
pm_buffer = {}      # key = "PM1" ... "PM5"  value = dict data
EXPECTED_PM_COUNT = 25

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

    if request.method == "POST":

        clen = request.META.get("CONTENT_LENGTH")
        if not clen:
            return JsonResponse({"ok": False})

        clen = int(clen)
        body_bytes = request.read(clen)

        raw = body_bytes.decode('utf-8', errors='ignore')
        logging.info(f"data: {raw}")

        try:
            body = json.loads(raw)
        except Exception:
            logging.warning("invalid json")
            return JsonResponse({"ok": False, "invalid_json": True})

        dev = body.get("device")
        arr = body.get("data")

        if not isinstance(arr, list):
            return JsonResponse({"ok": False, "data_not_list": True})

        # flatten fields
        point = Point("plc_data").tag("device", dev)

        for row in arr:
            meter = row.get("meter")
            if not meter:
                continue

            # mapping singkat PLC → server format
            p = float(row.get("p", row.get("Power", 0)))
            v = float(row.get("v", row.get("Voltage", 0)))
            k = float(row.get("k", row.get("Kwh", 0)))
            c = float(row.get("c", row.get("Current", 0)))

            point = point.field(f"{meter}_Power",   p)
            point = point.field(f"{meter}_Voltage", v)
            point = point.field(f"{meter}_Kwh",     k)
            point = point.field(f"{meter}_Current", c)

        write_api.write(
            bucket=settings.INFLUXDB["bucket"],
            org=settings.INFLUXDB["org"],
            record=point
        )

        logging.info(f"=== {len(arr)} PM pushed ===")

        return JsonResponse({"ok": True, "count": len(arr)})

    return JsonResponse({"ok": True})
