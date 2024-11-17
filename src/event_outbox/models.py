import uuid

from django.db import models
from django.utils import timezone


class EventOutbox(models.Model):
    id = models.BigAutoField(primary_key=True)
    event_id = models.UUIDField(default=uuid.uuid4, unique=True, editable=False)
    event_type = models.CharField(max_length=255)
    event_date_time = models.DateTimeField(default=timezone.now)
    environment = models.CharField(max_length=255)
    event_context = models.JSONField()
    metadata_version = models.PositiveBigIntegerField(default=1)
    retry_count = models.IntegerField(default=0)
    status = models.CharField(
        max_length=20,
        choices=[
            ('PENDING', 'Pending'),
            ('PROCESSING', 'Processing'),
            ('FAILED', 'Failed'),
        ],
        default='PENDING',
    )
    last_error = models.TextField(null=True, blank=True)

    class Meta:
        indexes = [
            models.Index(fields=['event_date_time']),
            models.Index(fields=['event_type']),
            models.Index(fields=['status']),
        ]
        ordering = ['event_date_time']

    def __str__(self) -> str:
        return f"{self.event_type} at {self.event_date_time}"
