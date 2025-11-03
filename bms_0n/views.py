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
        raw = request.body.decode(errors='ignore')
        logging.info(f"data: {raw}")

        try:
            body = json.loads(raw)
            dev = body.get("device")
            arr = body.get("data", [])

            for row in arr:
                point = (
                    Point("power_meter")
                    .tag("device", dev)
                    .tag("meter", row.get("meter"))
                    .field("Power",   float(row.get("Power")))
                    .field("Voltage", float(row.get("Voltage")))
                    .field("Current", float(row.get("Current")))
                )

                write_api.write(
                    bucket=settings.INFLUXDB["bucket"],
                    org=settings.INFLUXDB["org"],
                    record=point
                )

        except Exception as e:
            logging.exception("influx write error")
            return JsonResponse({"ok": False, "error": str(e)}, status=500)

    forwarded = request.META.get('HTTP_X_FORWARDED_FOR')
    remote = request.META.get('REMOTE_ADDR')

    return JsonResponse({
        "ok": True,
        "X-Forwarded-For": forwarded,
        "REMOTE_ADDR": remote
    })