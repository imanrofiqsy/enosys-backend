import os
import django
import random
import time
import sys

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'api.settings')
django.setup()

from bms_0n.websocket_broadcast import kirim_data_dashboard

print("✅ dummy_data.py started", flush=True)

while True:
    value = random.randint(0, 100)
    print(f"📤 Sending dummy data: {value}", flush=True)
    try:
        kirim_data_dashboard(value, "%")
    except Exception as e:
        print("❌ Error:", e, file=sys.stderr, flush=True)
    time.sleep(5)
