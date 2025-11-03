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
        if text_data:
            await self.send(text_data=json.dumps({"message": "Echo: " + text_data}))

    # handler event dari group_send; tipe harus sama: send_dashboard_data
    async def send_dashboard_data(self, event):
        data = event.get("data", {})
        # kirim string JSON ke client
        await self.send(text_data=json.dumps({"type": "dashboard_update", "payload": data}))
