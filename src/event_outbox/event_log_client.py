from contextlib import contextmanager
from typing import Any, Generator

import clickhouse_connect
import structlog
from clickhouse_connect.driver.exceptions import DatabaseError
from django.conf import settings
from sentry_sdk import capture_exception

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

    def insert(self, data: list[tuple[Any, ...]]) -> None:
        logger.debug("Inserting data into ClickHouse", data_length=len(data))
        try:
            self._client.insert(
                table=f"{settings.CLICKHOUSE_SCHEMA}.{settings.CLICKHOUSE_EVENT_LOG_TABLE_NAME}",
                data=data,
                column_names=EVENT_LOG_COLUMNS,
            )
            logger.info("Successfully inserted data into ClickHouse", count=len(data))
        except DatabaseError as e:
            logger.error('Unable to insert data into ClickHouse', error=str(e))
            capture_exception(e)
            raise
