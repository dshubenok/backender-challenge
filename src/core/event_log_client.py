import re
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

import clickhouse_connect
import structlog
from clickhouse_connect.driver.exceptions import DatabaseError
from django.conf import settings
from django.utils import timezone

from core.base_model import Model

logger = structlog.get_logger(__name__)

EVENT_LOG_COLUMNS = [
    'event_type',
    'event_date_time',
    'environment',
    'event_context',
]


class EventLogClient:
    def __init__(self, client: clickhouse_connect.driver.Client) -> None:
        self._client = client

    @classmethod
    @contextmanager
    def init(cls) -> Generator['EventLogClient', None, None]:
        client = clickhouse_connect.get_client(
            host=settings.CLICKHOUSE_HOST,
            port=settings.CLICKHOUSE_PORT,
            username=settings.CLICKHOUSE_USER,
            password=settings.CLICKHOUSE_PASSWORD,
            query_retries=2,
            connect_timeout=30,
            send_receive_timeout=10,
        )
        try:
            yield cls(client)
        except Exception as e:
            logger.error('Error while executing ClickHouse query', error=str(e))
            raise
        finally:
            client.close()

    def insert(self, data: list[Model] | list[tuple[Any, ...]]) -> None:
        if not data:
            return

        if isinstance(data[0], Model):
            data = self._convert_data(data)

        try:
            self._client.insert(
                table=f"{settings.CLICKHOUSE_SCHEMA}.{settings.CLICKHOUSE_EVENT_LOG_TABLE_NAME}",
                data=data,
                column_names=EVENT_LOG_COLUMNS,
            )
            logger.info("Successfully inserted data into ClickHouse", count=len(data))
        except DatabaseError as e:
            logger.error('Unable to insert data into ClickHouse', error=str(e))
            raise

    def query(self, query: str) -> list[tuple[Any, ...]]:
        logger.debug('Executing ClickHouse query', query=query)
        try:
            return self._client.query(query).result_rows
        except DatabaseError as e:
            logger.error('Failed to execute ClickHouse query', error=str(e))
            return []

    def _convert_data(self, data: list[Model]) -> list[tuple[Any, ...]]:
        return [
            (
                self._to_snake_case(event.__class__.__name__),
                timezone.now(),
                settings.ENVIRONMENT,
                event.model_dump_json(),
            )
            for event in data
        ]

    def _to_snake_case(self, event_name: str) -> str:
        result = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', event_name)
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', result).lower()
