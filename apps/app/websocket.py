import datetime
import pytz

from channels.generic.websocket import AsyncWebsocketConsumer
from websockets.exceptions import ConnectionClosedOK

from apps.app.db_models import Log

tz = pytz.timezone('Europe/Stockholm')


class Consumer(AsyncWebsocketConsumer):
    groupId = 'group'

    """
    Helper method
    """
    async def send_data(self, text_data):
        event = f"{datetime.datetime.now(tz).strftime('%Y-%m-%dT%H:%M:%S.%f')} - TMDB - {text_data}"
        Log(type="log", message=text_data).save()
        try:
            await self.send(text_data=event)
        except ConnectionClosedOK:
            print("Client has disconnected from ws. Ignoring")

    async def connect(self):
        await self.channel_layer.group_add(self.groupId, self.channel_name)
        await self.accept()
        for log in (Log.objects.filter(type='log')
                    .order_by('timestamp')
                    .limit(1000)
                    .only('timestamp', 'message')):
            event = f"{log.timestamp.isoformat()} - TMDB - {log.message}"
            await self.send(text_data=event)
        else:
            await self.send_data("Connected")

    async def disconnect(self, close_code):
        await self.channel_layer.group_discard(self.groupId, self.channel_name)

    async def receive(self, text_data):
        Log(type="log", message=text_data).save()
        await self.channel_layer.group_send(self.groupId, {"type": "events", "message": text_data})

    """
    Triggers on groupsends to type: events
    """
    async def events(self, event):
        await self.send_data(event['message'])
