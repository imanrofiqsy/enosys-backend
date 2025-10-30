from channels.generic.websocket import AsyncWebsocketConsumer
import json

class MyConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        await self.accept()
        await self.send(text_data=json.dumps({"message": "Connected"}))

    async def receive(self, text_data):
        await self.send(text_data=json.dumps({"message": "Echo: " + text_data}))

    # Handler event dari kirim_data_dashboard()
    async def send_dashboard_data(self, event):
        data = event["data"]
        await self.send(text_data=json.dumps(data))