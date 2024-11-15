from django.apps import AppConfig


class EventOutboxConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'event_outbox'
