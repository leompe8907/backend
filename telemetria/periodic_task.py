from django_celery_beat.models import PeriodicTask, CrontabSchedule
from datetime import datetime

schedule, created = CrontabSchedule.objects.get_or_create(
    minute='50',
    hour='16',
    day_of_week='*',
    day_of_month='*',
    month_of_year='*',
)

PeriodicTask.objects.update_or_create(
    crontab=schedule,
    name='Test fetch and store telemetry data',
    defaults={'task': 'telemetria.tasks.test_fetch_store_telemetry'},
)

PeriodicTask.objects.update_or_create(
    crontab=schedule,
    name='Update data OTT',
    defaults={'task': 'telemetria.tasks.update_data_ott'},
)

PeriodicTask.objects.update_or_create(
    crontab=schedule,
    name='Update data DVB',
    defaults={'task': 'telemetria.tasks.update_data_dvb'},
)

PeriodicTask.objects.update_or_create(
    crontab=schedule,
    name='Update data End Catchup',
    defaults={'task': 'telemetria.tasks.update_data_end_catchup'},
)

PeriodicTask.objects.update_or_create(
    crontab=schedule,
    name='Update data Stop VOD',
    defaults={'task': 'telemetria.tasks.update_data_stop_vod'},
)

PeriodicTask.objects.update_or_create(
    crontab=schedule,
    name='Update data End VOD',
    defaults={'task': 'telemetria.tasks.update_data_end_vod'},
)
