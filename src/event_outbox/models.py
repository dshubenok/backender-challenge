from django.db import models
from django.utils import timezone


class EventOutbox(models.Model):
    event_type = models.CharField(max_length=255)
    event_date_time = models.DateTimeField(default=timezone.now)
    environment = models.CharField(max_length=255)
    event_context = models.JSONField()
    metadata_version = models.PositiveBigIntegerField(default=1)

    def __str__(self) -> str:
        return f"{self.event_type} at {self.event_date_time}"
