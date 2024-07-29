## permite convertir los modelos en json
from rest_framework import serializers
from .models import Telemetria, MergedTelemetricOTT, MergedTelemetricDVB, MergedTelemetricStopCatchup, MergedTelemetricEndCatchup, MergedTelemetricStopVOD, MergedTelemetricEndVOD

class TelemetriaSerializer(serializers.ModelSerializer):
    class Meta:
        model = Telemetria
        fields = '__all__'

class MergedTelemetricOTTSerializer(serializers.ModelSerializer):
    class Meta:
        model = MergedTelemetricOTT
        fields = '__all__'

class MergedTelemetricDVBSerializer(serializers.ModelSerializer):
    class Meta:
        model = MergedTelemetricDVB
        fields = '__all__'

class MergedTelemetricStopCatchupSerializer(serializers.ModelSerializer):
    class Meta:
        model = MergedTelemetricStopCatchup
        fields = '__all__'

class MergedTelemetricEndCatchupSerializer(serializers.ModelSerializer):
    class Meta:
        model = MergedTelemetricEndCatchup
        fields = '__all__'

class MergedTelemetricStopVODSerializer(serializers.ModelSerializer):
    class Meta:
        model = MergedTelemetricStopVOD
        fields = '__all__'

class MergedTelemetricEndVODSerializer(serializers.ModelSerializer):
    class Meta:
        model = MergedTelemetricEndVOD
        fields = '__all__'
