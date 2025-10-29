from channels.generic.websocket import AsyncWebsocketConsumer
import json
import logging

class MyConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        logging.info(f"🔌 New WebSocket connection from {self.scope['client']}")
        await self.accept()
        await self.send(text_data=json.dumps({"message": "Connected"}))

    async def disconnect(self, close_code):
        logging.info(f"❌ WebSocket disconnected ({close_code})")

    async def receive(self, text_data):
        logging.info(f"📩 Received: {text_data}")
        await self.send(text_data=json.dumps({"message": "Echo: " + text_data}))