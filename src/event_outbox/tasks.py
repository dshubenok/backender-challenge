import json
from typing import Any

import structlog
from celery import Task, shared_task
from django.conf import settings
from django.db import models, transaction

from core.event_log_client import EventLogClient
from event_outbox.models import EventOutbox

logger = structlog.get_logger(__name__)


@shared_task(
    bind=True,
    max_retries=3,
    retry_backoff=True,
    autoretry_for=(Exception,),
    default_retry_delay=60,
)
def process_event_outbox(self: Task) -> None:
    logger = structlog.get_logger(__name__).bind(
        task='process_event_outbox',
        retry_count=self.request.retries,
    )
    logger.info("Starting event processing task")

    try:
        with transaction.atomic():
            events = list(
                EventOutbox.objects.select_for_update(skip_locked=True)
                .filter(status__in=['PENDING', 'FAILED'])
                .order_by('event_date_time')[:settings.CLICKHOUSE_BATCH_SIZE],
            )

            if not events:
                logger.info("No events to process")
                return

            event_ids = [event.id for event in events]

            EventOutbox.objects.filter(id__in=event_ids).update(
                status='PROCESSING',
            )

        data = prepare_event_data(events)

        try:
            with EventLogClient.init() as client:
                client.insert(data)

            with transaction.atomic():
                EventOutbox.objects.filter(id__in=event_ids).delete()

            logger.info(
                "Successfully processed events",
                count=len(events),
            )
        except Exception as e:
            with transaction.atomic():
                EventOutbox.objects.filter(id__in=event_ids).update(
                    status='FAILED',
                    last_error=str(e),
                    retry_count=models.F('retry_count') + 1,
                )
            logger.error(
                "Failed to process events",
                error=str(e),
                event_count=len(events),
                retry_count=self.request.retries,
            )
            raise

    except Exception as e:
        logger.error(
            "Task execution failed",
            error=str(e),
            retry_count=self.request.retries,
        )
        raise


def prepare_event_data(events: list[EventOutbox]) -> list[tuple[Any, ...]]:
    return [
        (
            event.event_type,
            event.event_date_time,
            event.environment,
            json.dumps(event.event_context),
        )
        for event in events
    ]
