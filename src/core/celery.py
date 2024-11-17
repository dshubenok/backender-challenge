import os
from celery import Celery

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'core.settings')

app = Celery('core')

app.config_from_object('django.conf:settings', namespace='CELERY')
app.autodiscover_tasks()

from celery.schedules import crontab

app.conf.beat_schedule = {
    'process-event-outbox-every-minute': {
        'task': 'event_outbox.tasks.process_event_outbox',
        'schedule': crontab(minute='*/1'),
    },
}
