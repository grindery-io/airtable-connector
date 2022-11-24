import json
import requests
from pyairtable import Table
from rest_framework.generics import GenericAPIView
from rest_framework.response import Response
from rest_framework import status

from .serializers import ConnectorSerializer
from common.serializers import serialize_base, serialize_table


class TriggerTableView(GenericAPIView):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.background_tasks = set()

    serializer_class = ConnectorSerializer

    def post(self, request):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        params = serializer.data.get('params')
        request_id = serializer.data.get('id')

        personal_access_token = ''
        app_id = ''
        table_name = ''

        key = params['key']
        print('--------fieldData--------', params['fieldData'])

        if 'personal_access_token' in params['fieldData']:
            personal_access_token = params['fieldData']['personal_access_token']
        if 'app_id' in params['fieldData']:
            app_id = params['fieldData']['app_id']
        if 'table_name' in params['fieldData']:
            table_name = params['fieldData']['table_name']

        print('--------personal_access_token--------', personal_access_token)
        print('--------app_id--------', app_id)
        print('--------table_name--------', table_name)

        app_list = []
        table_list = []
        if personal_access_token != '' and app_id == '' and table_name == '':
            header_my = {
                'Authorization': 'Bearer ' + personal_access_token,
                'Content-Type': 'application/json'
            }
            url = "https://api.airtable.com/v0/meta/bases"
            res_app_list = requests.get(headers=header_my, url=url)

            if res_app_list.status_code == 200:
                for an_app in json.loads(res_app_list.content)['bases']:
                    app_list.append({
                        **serialize_base(an_app)
                    })

            return Response(
                {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {
                        "inputFields": [
                            {
                                "key": "personal_access_token",
                                "label": "Airtable Personal access token",
                                "type": "string",
                                "placeholder": "patTPTGCSDECZm7i.3fa8581g1r52a52cb5a224sb8arccaf2ae82df346cd685ee766ff939dcb879be6",
                                "helpText": "To get Personal access token, visit your account page: https://airtable.com/create/tokens.",
                                "list": False,
                                "required": True,
                                "default": "",
                                "computed": False
                            },
                            {
                                "key": "app_id",
                                "label": "Base",
                                "helpText": "",
                                "type": "string",
                                "required": True,
                                "placeholder": "Choose...",
                                "choices": app_list
                            }
                        ]
                    }
                },
                status=status.HTTP_201_CREATED
            )

        elif personal_access_token != '' and app_id != '' and table_name == '':
            header_my = {
                'Authorization': 'Bearer ' + personal_access_token,
                'Content-Type': 'application/json'
            }
            url = "https://api.airtable.com/v0/meta/bases"
            res_app_list = requests.get(headers=header_my, url=url)

            if res_app_list.status_code == 200:
                for an_app in json.loads(res_app_list.content)['bases']:
                    app_list.append({
                        **serialize_base(an_app)
                    })

            header = {
                'Authorization': 'Bearer ' + personal_access_token,
                'Content-Type': 'application/json'
            }
            url = "https://api.airtable.com/v0/meta/bases/" + app_id + "/tables"
            res_table_list = requests.get(headers=header, url=url)

            if res_table_list.status_code == 200:
                for a_table in json.loads(res_table_list.content)['tables']:
                    table_list.append({
                        **serialize_table(a_table)
                    })

            return Response(
                {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {
                        "inputFields": [
                            {
                                "key": "personal_access_token",
                                "label": "Airtable Personal access token",
                                "type": "string",
                                "placeholder": "patTPTGCSDECZm7i.3fa8581g1r52a52cb5a224sb8arccaf2ae82df346cd685ee766ff939dcb879be6",
                                "helpText": "To get Personal access token, visit your account page: https://airtable.com/create/tokens.",
                                "list": False,
                                "required": True,
                                "default": "",
                                "computed": False
                            },
                            {
                                "key": "app_id",
                                "label": "Base",
                                "helpText": "",
                                "type": "string",
                                "required": True,
                                "placeholder": "Choose...",
                                "choices": app_list
                            },
                            {
                                "key": "table_name",
                                "label": "Table",
                                "helpText": "Table must have at least one record with data. If your table is empty, please add an example record.",
                                "type": "string",
                                "required": True,
                                "placeholder": "Choose...",
                                "choices": table_list
                            }
                        ]
                    }
                },
                status=status.HTTP_201_CREATED
            )

        elif personal_access_token != '' and app_id != '' and table_name != '':
            try:
                table = Table(personal_access_token, app_id, table_name)
                sample_array = {}
                out_put_fields = []
                for key in table.first()['fields']:
                    sample_array[key.replace(" ", "_")] = table.first()['fields'][key]
                    out_put_fields.append({
                        "key": key.replace(" ", "_"),
                        "label": key,
                        "type": "string"
                    })
                return Response(
                    {
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "result": {
                            "sample": sample_array,
                            "outputFields": out_put_fields
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
                    status=status.HTTP_400_BAD_REQUEST
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
                status=status.HTTP_400_BAD_REQUEST
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
                if table.first():
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
                                        "placeholder": "keyLOfGeYHXLqpeo0",
                                        "helpText": "To get API key, visit your account page: https://airtable.com/account.",
                                        "list": False,
                                        "required": True
                                    },
                                    {
                                        "key": "app_id",
                                        "label": "Base Id",
                                        "type": "string",
                                        "placeholder": "appGgI109POdPOOKK",
                                        "helpText": "You can get base ID from the base URL, or from API documentation at https://airtable.com/api.",
                                        "list": False,
                                        "required": True
                                    },
                                    {
                                        "key": "table_name",
                                        "label": "Table Name",
                                        "type": "string",
                                        "placeholder": "Table Name",
                                        "helpText": "Table must have at least one record with data. If your table is empty, please add an example record.",
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
                            "jsonrpc": "2.0",
                            "id": request_id,
                            "result": {
                                "inputFields": [
                                    {
                                        "key": "api_key",
                                        "label": "API key",
                                        "type": "string",
                                        "placeholder": "keyLOfGeYHXLqpeo0",
                                        "helpText": "To get API key, visit your account page: https://airtable.com/account.",
                                        "list": False,
                                        "required": True
                                    },
                                    {
                                        "key": "app_id",
                                        "label": "Base Id",
                                        "type": "string",
                                        "placeholder": "appGgI109POdPOOKK",
                                        "helpText": "You can get base ID from the base URL, or from API documentation at https://airtable.com/api.",
                                        "list": False,
                                        "required": True
                                    },
                                    {
                                        "key": "table_name",
                                        "label": "Table Name",
                                        "type": "string",
                                        "placeholder": "Table Name",
                                        "helpText": "Table must have at least one record with data. If your table is empty, please add an example record.",
                                        "list": False,
                                        "required": True
                                    },
                                    {
                                        "key": "warning",
                                        "label": "Warning",
                                        "type": "info",
                                        "helpText": "Table must have at least one record with data. If your table is empty, please add an example record."
                                    }
                                ],
                            }
                        },
                        status=status.HTTP_400_BAD_REQUEST
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
                    status=status.HTTP_400_BAD_REQUEST
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
                status=status.HTTP_400_BAD_REQUEST
            )


def get_id_list(api_key, app_id, table_name):
    try:
        id_list = []
        for record in Table(api_key, app_id, table_name).all():
            id_list.append(record['id'])
        return id_list
    except:
        return []


def get_new_rows(api_key, app_id, table_name, added_id_list):
    added_records = []
    try:
        for item in Table(api_key, app_id, table_name).all():
            if item['id'] in added_id_list:
                added_records.append(item['fields'])
    except:
        added_records = []
    rows_objects = []
    for row in added_records:
        row_object = {}
        for key in row:
            row_object[key.replace(" ", "_")] = row[key]
        rows_objects.append(row_object)
    return rows_objects
