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

        body_bytes = request.read(clen)  # <-- baca FULL sesuai length

        if len(body_bytes) < clen:
            logging.warning("payload truncated - skip")
            return JsonResponse({"ok": False, "partial": True})

        raw = body_bytes.decode('utf-8', errors='ignore')
        logging.info(f"data: {raw}")

        try:
            body = json.loads(raw)
        except Exception:
            logging.warning("json incomplete / cannot decode - skip")
            return JsonResponse({"ok": False, "invalid_json": True})

        # --- parse and write influx ---
        try:
            dev = body.get("device")
            arr = body.get("data", [])

            for row in arr:
                point = (
                    Point("plc_data")
                    .tag("device", dev)
                    .tag("meter", row.get("meter"))
                    .field("Power", float(row.get("Power")))
                    .field("Voltage", float(row.get("Voltage")))
                    .field("Current", float(row.get("Current")))
                )
                write_api.write(
                    bucket=settings.INFLUXDB["bucket"],
                    org=settings.INFLUXDB["org"],
                    record=point
                )
                logging.info("data terkirim")

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