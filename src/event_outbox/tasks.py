import json

import structlog
from celery import shared_task

from core.event_log_client import EventLogClient
from event_outbox.models import EventOutbox

logger = structlog.get_logger(__name__)


@shared_task
def process_event_outbox() -> None:
    logger = structlog.get_logger(__name__).bind(task='process_event_outbox')
    logger.info("Starting task")

    events = list(EventOutbox.objects.all()[:1000])

    if not events:
        logger.info("No events to process")
        return

    data = [
        (
            event.event_type,
            event.event_date_time,
            event.environment,
            json.dumps(event.event_context),
        )
        for event in events
    ]

    with EventLogClient.init() as client:
        try:
            client.insert(data)
            EventOutbox.objects.filter(
                id__in=[event.id for event in events],
            ).delete()
            logger.info("Successfully processed events", count=len(events))
        except Exception as e:
            logger.error("Failed to process event outbox", error=str(e))

    logger.info("Completed process_event_outbox task")
