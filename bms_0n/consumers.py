# bms_0n/consumers.py
from channels.generic.websocket import AsyncWebsocketConsumer
import json
import logging
from .utils import send_data_single

logger = logging.getLogger(__name__)

class MyConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        self.group_name = "dashboard_group"
        await self.channel_layer.group_add(self.group_name, self.channel_name)
        await self.accept()
        await self.send(text_data=json.dumps({"message": "Connected"}))
        logger.info("WS connected: %s", self.channel_name)

    async def disconnect(self, close_code):
        await self.channel_layer.group_discard(self.group_name, self.channel_name)
        logger.info("WS disconnected: %s code=%s", self.channel_name, close_code)

    async def receive(self, text_data=None, bytes_data=None):
        logger.debug("WS received raw: %s", text_data)

        if not text_data:
            return

        # Parse JSON dari frontend
        try:
            data = json.loads(text_data)
        except json.JSONDecodeError:
            await self.send(text_data=json.dumps({"error": "Invalid JSON"}))
            return

        # Ambil nilai
        payload = data.get("payload")
        device = payload.get("device") if payload else None
        target = payload.get("target") if payload else None
        value = payload.get("value") if payload else None

        topic = data.get("topic")
        if topic == "request_data":
            send_data_single.build_dashboard_payload()

        # Echo kembali ke client (bisa diubah sesuai kebutuhan)
        await self.send(text_data=json.dumps({
            "type": "ping",
            "topic": "ping",
            "payload": data
        }))

    # handler event dari group_send; tipe harus sama: send_dashboard_data
    async def send_dashboard_data(self, event):
        data = event.get("data", {})
        # kirim string JSON ke client
        await self.send(text_data=json.dumps({"type": "dashboard_update", "payload": data}))

    async def power_summary(self, event):
        data = event.get("data", {})
        await self.send(text_data=json.dumps({
            "type": "power_summary",
            "topic": "power",
            "payload": data
        }))

    async def alarms_status(self, event):
        data = event.get("data", {})
        await self.send(text_data=json.dumps({
            "type": "alarms_status",
            "topic": "alarms",
            "payload": data
        }))

    async def solar_data(self, event):
        data = event.get("data", {})
        await self.send(text_data=json.dumps({
            "type": "solar_data",
            "topic": "solar",
            "payload": data
        }))

    async def realtime_chart(self, event):
        data = event.get("data", {})
        await self.send(text_data=json.dumps({
            "type": "realtime_chart",
            "topic": "energy",
            "payload": data
        }))

    async def weekly_chart(self, event):
        data = event.get("data", {})
        await self.send(text_data=json.dumps({
            "type": "weekly_chart",
            "topic": "pln_vs_solar",
            "payload": data
        }))

    async def overview_room(self, event):
        data = event.get("data", {})
        await self.send(text_data=json.dumps({
            "type": "overview_room",
            "topic": "floor_status",
            "payload": data
        }))

    async def system_status(self, event):
        data = event.get("data", {})
        await self.send(text_data=json.dumps({
            "type": "system_status",
            "topic": "system_status",
            "payload": data
        }))

    async def ping(self, event):
        data = event.get("data", {})
        await self.send(text_data=json.dumps({
            "type": "ping",
            "topic": "ping",
            "payload": data
        }))

    async def broadcast_message(self, event):
        """
        Handler yang dipanggil oleh group_send
        """
        # Kirim ke semua client
        await self.send(text_data=json.dumps({
            "event": event["event"],
            "payload": event["payload"],
            "topic": event["topic"]
        }))

    async def room_status(self, event):
        data = event.get("data", {})
        await self.send(text_data=json.dumps({
            "type": "room_status",
            "topic": "room_status",
            "payload": data
        }))