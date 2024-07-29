from django_cron import CronJobBase, Schedule
from .tasks import test_fetch_store_telemetry, update_data_ott, update_data_dvb, update_data_end_catchup, update_data_stop_vod, update_data_end_vod

class TestFetchStoreTelemetryCronJob(CronJobBase):
    RUN_EVERY_MINS = 30  # Cada 40 minutos

    schedule = Schedule(run_every_mins=RUN_EVERY_MINS)
    code = 'telemetria.test_fetch_store_telemetry'  # un código único

    def do(self):
        test_fetch_store_telemetry()

class UpdateDataOttCronJob(CronJobBase):
    RUN_EVERY_MINS = 40

    schedule = Schedule(run_every_mins=RUN_EVERY_MINS)
    code = 'telemetria.update_data_ott'

    def do(self):
        update_data_ott()

class UpdateDataDvbCronJob(CronJobBase):
    RUN_EVERY_MINS = 40

    schedule = Schedule(run_every_mins=RUN_EVERY_MINS)
    code = 'telemetria.update_data_dvb'

    def do(self):
        update_data_dvb()

class UpdateDataEndCatchupCronJob(CronJobBase):
    RUN_EVERY_MINS = 40

    schedule = Schedule(run_every_mins=RUN_EVERY_MINS)
    code = 'telemetria.update_data_end_catchup'

    def do(self):
        update_data_end_catchup()

class UpdateDataStopVodCronJob(CronJobBase):
    RUN_EVERY_MINS = 40

    schedule = Schedule(run_every_mins=RUN_EVERY_MINS)
    code = 'telemetria.update_data_stop_vod'

    def do(self):
        update_data_stop_vod()

class UpdateDataEndVodCronJob(CronJobBase):
    RUN_EVERY_MINS = 40

    schedule = Schedule(run_every_mins=RUN_EVERY_MINS)
    code = 'telemetria.update_data_end_vod'

    def do(self):
        update_data_end_vod()
