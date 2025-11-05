from django.http import HttpResponse, JsonResponse
import json
from django.views.decorators.csrf import csrf_exempt
import logging
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from django.conf import settings

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
            return JsonResponse({"ok": False, "no_content_length": True})

        clen = int(clen)

        body_bytes = request.body
        raw = body_bytes.decode('utf-8', errors='ignore')


        if len(body_bytes) < clen:
            logging.warning("payload truncated - skip")
            return JsonResponse({"ok": False, "partial": True})

        raw = body_bytes.decode('utf-8', errors='ignore')
        logging.info(f"data raw: {raw}")

        try:
            raw_fixed = raw.replace("#NaN", "null").replace("#Inf", "null")
            logging.info(f"data fix: {raw_fixed}")
            body = json.loads(raw_fixed)
        except Exception:
            logging.warning("json incomplete / cannot decode - skip")
            return JsonResponse({"ok": False, "invalid_json": True})

        # --- parse and write influx ---
        try:
            dev = body.get("device")
            arr = body.get("data", [])

            # build fields untuk single point
            fields = {}

            for row in arr:
                meter = row.get("meter")  # contoh: "PM1"
                # bikin field nama: "PM1_Power", "PM1_Voltage", ...
                fields[f"{meter}_Power"]   = float(row.get("Power"))
                fields[f"{meter}_Voltage"] = float(row.get("Voltage"))
                fields[f"{meter}_Current"] = float(row.get("Current"))
                fields[f"{meter}_Kwh"] = float(row.get("Kwh"))

            # buat 1 point saja
            point = (
                Point("plc_data")
                .tag("device", dev)
            )

            for k, v in fields.items():
                point = point.field(k, v)

            write_api.write(
                bucket=settings.INFLUXDB["bucket"],
                org=settings.INFLUXDB["org"],
                record=point
            )

            logging.info("data terkirim (flattened)")

        except Exception as e:
            logging.exception("influx write error")
            return JsonResponse({"ok": False, "error": str(e)}, status=500)

    forwarded = request.META.get('HTTP_X_FORWARDED_FOR')
    remote = request.META.get('REMOTE_ADDR')

    return JsonResponse({
        "ok": True,
        "forwarded": forwarded,
        "addr": remote
    })