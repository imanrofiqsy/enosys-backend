# bms_0n/consumers.py
from channels.generic.websocket import AsyncWebsocketConsumer
import json
import logging

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
        # optional echo, atau kita bisa ignore
        logger.debug("WS received data: %s", text_data)
        if text_data:
            await self.send(text_data=json.dumps({"message": "Echo: " + text_data}))

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
