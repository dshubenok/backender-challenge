import uuid
from unittest.mock import patch

import pytest
from clickhouse_connect.driver import Client
from clickhouse_connect.driver.exceptions import DatabaseError
from django.conf import settings
from django.db import IntegrityError, transaction

from event_outbox.models import EventOutbox
from event_outbox.tasks import process_event_outbox
from users.use_cases import CreateUser, CreateUserRequest

pytestmark = [pytest.mark.django_db]


@pytest.fixture
def use_case() -> CreateUser:
    return CreateUser()


@pytest.fixture(autouse=True)
def clean_clickhouse(f_ch_client: Client) -> None:
    table = f"{settings.CLICKHOUSE_SCHEMA}.{settings.CLICKHOUSE_EVENT_LOG_TABLE_NAME}"
    f_ch_client.command(f"TRUNCATE TABLE {table}")
    yield
    f_ch_client.command(f"TRUNCATE TABLE {table}")


class TestUserCreation:
    def test_creates_user_successfully(self, use_case: CreateUser) -> None:
        request = CreateUserRequest(
            email='test@example.com',
            first_name='Test',
            last_name='User',
        )
        response = use_case.execute(request)

        assert response.result.email == 'test@example.com'
        assert response.error == ''

    def test_prevents_duplicate_email(self, use_case: CreateUser) -> None:
        request = CreateUserRequest(
            email='test@example.com',
            first_name='Test',
            last_name='User',
        )

        use_case.execute(request)
        response = use_case.execute(request)

        assert response.result is None
        assert response.error == 'User with this email already exists'


class TestEventOutbox:
    def test_event_stored_on_user_creation(self, use_case: CreateUser) -> None:
        request = CreateUserRequest(
            email='test@example.com',
            first_name='Test',
            last_name='User',
        )
        response = use_case.execute(request)

        assert response.result.email == 'test@example.com'
        assert response.error == ''

        events = EventOutbox.objects.all()
        assert events.count() == 1

        event = events.first()
        assert event.event_type == 'user_created'
        assert event.event_context['email'] == 'test@example.com'
        assert event.event_context['first_name'] == 'Test'
        assert event.event_context['last_name'] == 'User'

    def test_event_not_stored_on_failure(self, use_case: CreateUser) -> None:
        with patch('users.models.User.objects.get_or_create') as mock:
            mock.side_effect = IntegrityError("Simulated error")
            request = CreateUserRequest(
                email='test@example.com',
                first_name='Test',
                last_name='User',
            )

            with pytest.raises(IntegrityError):
                use_case.execute(request)

        assert EventOutbox.objects.count() == 0


class TestClickHouseIntegration:
    def test_single_event_processing(
        self,
        use_case: CreateUser,
        f_ch_client: Client,
    ) -> None:
        email = f'test_{uuid.uuid4()}@example.com'
        request = CreateUserRequest(
            email=email,
            first_name='Test',
            last_name='User',
        )

        use_case.execute(request)
        process_event_outbox()

        query = """
            SELECT event_type, event_context
            FROM {schema}.{table}
            WHERE event_type = %(event_type)s
        """
        result = f_ch_client.query(
            query.format(
                schema=settings.CLICKHOUSE_SCHEMA,
                table=settings.CLICKHOUSE_EVENT_LOG_TABLE_NAME,
            ),
            parameters={'event_type': 'user_created'},
        )

        assert len(result.result_rows) == 1
        event_type, event_context = result.result_rows[0]
        assert event_type == 'user_created'
        assert email in event_context

    def test_batch_processing(
        self,
        use_case: CreateUser,
        f_ch_client: Client,
    ) -> None:
        for i in range(3):
            request = CreateUserRequest(
                email=f'test_{i}@example.com',
                first_name='Test',
                last_name='User',
            )
            use_case.execute(request)

        process_event_outbox()

        query = """
            SELECT count()
            FROM {schema}.{table}
            WHERE event_type = %(event_type)s
        """
        result = f_ch_client.query(
            query.format(
                schema=settings.CLICKHOUSE_SCHEMA,
                table=settings.CLICKHOUSE_EVENT_LOG_TABLE_NAME,
            ),
            parameters={'event_type': 'user_created'},
        )
        assert result.result_rows[0][0] == 3

    def test_handles_clickhouse_failure(
        self,
        use_case: CreateUser,
    ) -> None:
        request = CreateUserRequest(
            email='test@example.com',
            first_name='Test',
            last_name='User',
        )
        use_case.execute(request)

        with patch('core.event_log_client.EventLogClient.insert') as mock:
            mock.side_effect = DatabaseError("ClickHouse error")

            with pytest.raises(DatabaseError):
                process_event_outbox()

            event = EventOutbox.objects.first()
            assert event.status == 'FAILED'
            assert event.retry_count == 1
            assert 'ClickHouse error' in event.last_error

    def test_retries_failed_events(
        self,
        use_case: CreateUser,
    ) -> None:
        request = CreateUserRequest(
            email='test@example.com',
            first_name='Test',
            last_name='User',
        )
        use_case.execute(request)

        with transaction.atomic():
            EventOutbox.objects.all().update(
                status='FAILED',
                retry_count=1,
            )

        process_event_outbox()

        assert EventOutbox.objects.count() == 0
