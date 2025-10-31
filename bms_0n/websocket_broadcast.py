import json
from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer

def kirim_data_dashboard(total_power, cost):
    """
    Fungsi ini dapat dipanggil dari mana saja (views, task, script)
    untuk mengirim data ke semua klien dashboard.
    """
    channel_layer = get_channel_layer()
    async_to_sync(channel_layer.group_send)(
        "dashboard_group",
        {
            "type": "send_dashboard_data",  # harus sama dengan nama handler di consumer
            "data": {
                "total_power": str(total_power),
                "cost": cost,
            }
        }
    )