import os
import django
import random
import time
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'bms_0.settings')
django.setup()

from bms_0n.websocket_broadcast import kirim_data_dashboard

while True:
    kirim_data_dashboard(random.randint(0, 100), "%")
    time.sleep(5)