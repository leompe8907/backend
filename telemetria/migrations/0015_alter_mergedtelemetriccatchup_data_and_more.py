# Generated by Django 5.0.1 on 2024-03-18 16:15

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('telemetria', '0014_telemetria_datadate_telemetria_timedate'),
    ]

    operations = [
        migrations.AlterField(
            model_name='mergedtelemetriccatchup',
            name='data',
            field=models.CharField(blank=True, max_length=200, null=True),
        ),
        migrations.AlterField(
            model_name='mergedtelemetricdvb',
            name='data',
            field=models.CharField(blank=True, max_length=200, null=True),
        ),
        migrations.AlterField(
            model_name='mergedtelemetricott',
            name='data',
            field=models.CharField(blank=True, max_length=200, null=True),
        ),
    ]
