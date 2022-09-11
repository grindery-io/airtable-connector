import json
from pyairtable import Table
from rest_framework.generics import GenericAPIView
from rest_framework.response import Response
from rest_framework import status

from .serializers import ConnectorSerializer


class TriggerTableView(GenericAPIView):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.background_tasks = set()

    serializer_class = ConnectorSerializer

    def post(self, request):
        print('---------------1----------------')
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        params = serializer.data.get('params')
        request_id = serializer.data.get('id')

        api_key = ''
        app_id = ''
        table_name = ''

        print('---------------2----------------')
        key = params['key']
        if 'api_key' in params['fieldData']:
            api_key = params['fieldData']['api_key']
        if 'app_id' in params['fieldData']:
            app_id = params['fieldData']['app_id']
        if 'table_name' in params['fieldData']:
            table_name = params['fieldData']['table_name']

        print('---------------3----------------')
        if api_key != '' and app_id != '' and table_name != '':
            print('---------------4----------------', request.data)
            try:
                table = Table(api_key, app_id, table_name)
                sample_array = {}
                out_put_fields = []
                for key in table.first()['fields']:
                    sample_array[key.lower().replace(" ", "_")] = table.first()['fields'][key]
                return Response(
                    {
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "result": {
                            "sample": sample_array
                        }
                    },
                    status=status.HTTP_201_CREATED
                )
            except:
                return Response(
                    {
                        'jsonrpc': '2.0',
                        'error': {
                            'code': 1,
                            'message': 'Getting Table is failed'
                        },
                        'id': id
                    },
                    status=status.HTTP_201_CREATED
                )
        else:
            print('---------------5----------------')
            return Response(
                {
                    'jsonrpc': '2.0',
                    'error': {
                        'code': 1,
                        'message': 'fetching table is failed'
                    },
                    'id': request_id
                },
                status=status.HTTP_201_CREATED
            )


class FirstRowView(GenericAPIView):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.background_tasks = set()

    serializer_class = ConnectorSerializer

    def post(self, request):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        params = serializer.data.get('params')
        request_id = serializer.data.get('id')

        api_key = ''
        app_id = ''
        table_name = ''

        key = params['key']
        if 'api_key' in params['fieldData']:
            api_key = params['fieldData']['api_key']
        if 'app_id' in params['fieldData']:
            app_id = params['fieldData']['app_id']
        if 'table_name' in params['fieldData']:
            table_name = params['fieldData']['table_name']

        if api_key != '' and app_id != '' and table_name != '':
            try:
                table = Table(api_key, app_id, table_name)
                first_row = []
                if table.first()['fields'] != {}:
                    for key in table.first()['fields']:
                        first_row.append({
                            "key": "_" + key.replace(" ", "_"),
                            "label": key,
                            "type": "string"
                        })

                    return Response(
                        {
                            "jsonrpc": "2.0",
                            "id": request_id,
                            "result": {
                                "inputFields": [
                                    {
                                        "key": "api_key",
                                        "label": "API key",
                                        "type": "string",
                                        "placeholder": "To generate or manage your API key, visit your account page. https://airtable.com/account",
                                        "list": False,
                                        "required": True
                                    },
                                    {
                                        "key": "app_id",
                                        "label": "Base Id",
                                        "type": "string",
                                        "placeholder": "To get base id, API documentation of Base",
                                        "list": False,
                                        "required": True
                                    },
                                    {
                                        "key": "table_name",
                                        "label": "Table Name",
                                        "type": "string",
                                        "placeholder": "Enter table name in Airtable",
                                        "list": False,
                                        "required": True
                                    },
                                ] + first_row
                            }
                        },
                        status=status.HTTP_201_CREATED
                    )
                else:
                    return Response(
                        {
                            'jsonrpc': '2.0',
                            'error': {
                                'code': 1,
                                'message': 'Please add a sample record to get field list from your table'
                            },
                            'id': request_id
                        },
                        status=status.HTTP_201_CREATED
                    )
            except:
                return Response(
                    {
                        'jsonrpc': '2.0',
                        'error': {
                            'code': 1,
                            'message': 'Getting Table is failed'
                        },
                        'id': request_id
                    },
                    status=status.HTTP_201_CREATED
                )
        else:
            return Response(
                {
                    'jsonrpc': '2.0',
                    'error': {
                        'code': 1,
                        'message': 'fetching table is failed'
                    },
                    'id': request_id
                },
                status=status.HTTP_201_CREATED
            )


def get_number_of_rows(api_key, app_id, table_name):
    try:
        return len(Table(api_key, app_id, table_name).all())
    except:
        return 0


def get_new_rows(api_key, app_id, table_name, number_of_added_rows):
    rows = []
    try:
        for item in Table(api_key, app_id, table_name).all():
            rows.append(item['fields'])
    except:
        rows = []
    rows_objects = []
    for row in rows[len(rows) - number_of_added_rows: len(rows)]:
        row_object = {}
        for first_row, any_row in zip(rows[0], row):
            row_object[first_row.replace(" ", "_")] = any_row
        rows_objects.append(row_object)
    return rows_objects
