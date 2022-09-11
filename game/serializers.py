from django.utils.translation import ugettext_lazy as _
from rest_framework import serializers


class FieldDataSerializer(serializers.Serializer):
    api_key = serializers.CharField(required=False)
    app_id = serializers.CharField(required=False)
    table_name = serializers.CharField(required=False)

    class Meta:
        fields = ("guild", "channel")

    def validate(self, attrs):
        return attrs


class ParamSerializer(serializers.Serializer):
    key = serializers.CharField(required=False)
    fieldData = FieldDataSerializer()

    class Meta:
        fields = ("key",)

    def validate(self, attrs):
        return attrs


class ConnectorSerializer(serializers.Serializer):
    method = serializers.CharField()
    id = serializers.CharField()
    params = ParamSerializer()

    default_error_messages = {
        'invalid_type': _('type is invalid.'),
    }

    class Meta:
        fields = ("params", "method", "id")

    def validate(self, attrs):
        return attrs
