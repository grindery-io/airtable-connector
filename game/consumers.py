import json
from pyairtable import Table
import asyncio
from channels.generic.websocket import AsyncJsonWebsocketConsumer
from .views import get_number_of_rows, get_new_rows


class newAirtableRowTrigger:
    def __init__(self, socket, request):
        self.socket = socket
        self.request = request

    def start(self):
        return asyncio.create_task(newAirtableRowTrigger.main(self))

    async def main(self):
        request = json.loads(self.request)
        params = request.get("params", None)
        session_id = params['sessionId']
        api_key = ''
        app_id = ''
        table_name = ''

        if 'fields' in params:
            fields = params['fields']
            if 'api_key' in fields:
                api_key = fields['api_key']
            if 'app_id' in fields:
                app_id = fields['app_id']
            if 'table_name' in fields:
                table_name = fields['table_name']

        number_of_rows = get_number_of_rows(api_key, app_id, table_name)

        while self.socket.connected:
            print('--------Triggering--Airtable-------api_key---', api_key, '--------app_id-------', app_id,
                  '------table_name-------', table_name)
            check_number_of_row = get_number_of_rows(api_key, app_id, table_name)
            if check_number_of_row < number_of_rows:
                number_of_rows = check_number_of_row
            if check_number_of_row > number_of_rows:
                print('--------New-row-added----------api_key', api_key, '-----app_id----', app_id,
                      '------table_name------', table_name, '-------added-------', check_number_of_row - number_of_rows)
                response = get_new_rows(api_key, app_id, table_name, check_number_of_row - number_of_rows)
                number_of_rows = check_number_of_row
                for row in response:
                    await self.socket.send_json({
                        'jsonrpc': '2.0',
                        'method': 'notifySignal',
                        'params': {
                            'key': 'airtableNewRowTrigger',
                            'sessionId': session_id,
                            'payload': row
                        }
                    })
            await asyncio.sleep(60)


class SocketAdapter(AsyncJsonWebsocketConsumer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.background_tasks = set()
        self.connected = False

    async def connect(self):
        self.connected = True
        await self.accept()

    async def disconnect(self, close_code):
        self.connected = False
        print('-----socket disconnected-----')

    async def receive(self, text_data=None, bytes_data=None, **kwargs):
        request = json.loads(text_data)
        method = request.get("method", None)
        params = request.get("params", None)
        id = request.get("id", None)
        key = ''
        fields = ''
        session_id = ''
        api_key = ''
        app_id = ''
        table_name = ''

        if params:
            if 'key' in params:
                key = params['key']
            if 'sessionId' in params:
                session_id = params['sessionId']
            if 'fields' in params:
                fields = params['fields']
                if 'api_key' in fields:
                    api_key = fields['api_key']
                if 'app_id' in fields:
                    app_id = fields['app_id']
                if 'table_name' in fields:
                    table_name = fields['table_name']

        if method == 'ping':
            response = {
                'jsonrpc': '2.0',
                'result': {},
                'id': id
            }
            await self.send_json(response)

        if method == 'runAction':
            print('---------method--------')
            values = {}
            for key in fields:
                if key.startswith('_'):
                    values[key[1:].replace("_", " ")] = fields[key]
            try:
                table = Table(api_key, app_id, table_name)
                result = table.create(values, typecast=False)
                print('-----------------', result)
            except:
                fields = 'Error'
            run_action_response = {
                'jsonrpc': '2.0',
                'result': {
                    'key': 'airtableNewRowAction',
                    'sessionId': session_id,
                    'payload': fields
                },
                'id': id
            }
            await self.send_json(run_action_response)

        if method == 'setupSignal':
            self.background_tasks.add(newAirtableRowTrigger(self, text_data).start())
            response = {
                'jsonrpc': '2.0',
                'result': {},
                'id': id
            }
            await self.send_json(response)
