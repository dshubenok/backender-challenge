import json
import uuid
from unittest.mock import patch

import pytest
from clickhouse_connect.driver import Client
from django.conf import settings
from django.db import IntegrityError

from event_outbox.models import EventOutbox
from event_outbox.tasks import process_event_outbox
from users.use_cases import CreateUser, CreateUserRequest

pytestmark = [pytest.mark.django_db]


@pytest.fixture()
def f_use_case() -> CreateUser:
    return CreateUser()


@pytest.fixture(autouse=True)
def f_clean_up_event_log(f_ch_client: Client) -> None:
    f_ch_client.command(
        f"TRUNCATE TABLE {settings.CLICKHOUSE_SCHEMA}.{settings.CLICKHOUSE_EVENT_LOG_TABLE_NAME}"
    )
    yield
    f_ch_client.command(
        f"TRUNCATE TABLE {settings.CLICKHOUSE_SCHEMA}.{settings.CLICKHOUSE_EVENT_LOG_TABLE_NAME}"
    )


def test_user_created(f_use_case: CreateUser) -> None:
    request = CreateUserRequest(
        email='test@example.com', first_name='Test', last_name='Testovich',
    )

    response = f_use_case.execute(request)

    assert response.result.email == 'test@example.com'
    assert response.error == ''


def test_emails_are_unique(f_use_case: CreateUser) -> None:
    request = CreateUserRequest(
        email='test@example.com', first_name='Test', last_name='Testovich',
    )

    f_use_case.execute(request)
    response = f_use_case.execute(request)

    assert response.result is None
    assert response.error == 'User with this email already exists'


def test_event_log_entry_published(
    f_use_case: CreateUser,
    f_ch_client: Client,
) -> None:
    email = f'test_{uuid.uuid4()}@example.com'
    request = CreateUserRequest(
        email=email, first_name='Test', last_name='Testovich',
    )

    f_use_case.execute(request)

    process_event_outbox()

    query = f"""
    SELECT event_type, event_date_time, environment, event_context
    FROM {settings.CLICKHOUSE_SCHEMA}.{settings.CLICKHOUSE_EVENT_LOG_TABLE_NAME}
    WHERE event_type = 'user_created' AND event_context LIKE '%{email}%'
    """  # noqa: S608

    result = f_ch_client.query(query)

    assert len(result.result_rows) == 1
    row = result.result_rows[0]
    event_type, event_date_time, environment, event_context_json = row

    expected_event_context = {
        'email': email,
        'first_name': 'Test',
        'last_name': 'Testovich',
    }

    actual_event_context = json.loads(event_context_json)

    assert event_type == 'user_created'
    assert environment == settings.ENVIRONMENT
    assert actual_event_context == expected_event_context


def test_event_not_stored_if_transaction_fails(
    f_use_case: CreateUser,
) -> None:
    with patch('users.models.User.objects.get_or_create') as mocked_get_or_create:
        mocked_get_or_create.side_effect = IntegrityError("Simulated IntegrityError")
        request = CreateUserRequest(
            email='test_failure@example.com', first_name='Test', last_name='Testovich',
        )
        with pytest.raises(IntegrityError):
            f_use_case.execute(request)

    events_in_outbox = EventOutbox.objects.all()
    assert events_in_outbox.count() == 0


def test_event_stored_if_transaction_succeeds(
    f_use_case: CreateUser,
) -> None:
    request = CreateUserRequest(
        email='test_success@example.com', first_name='Test', last_name='Testovich',
    )
    response = f_use_case.execute(request)

    assert response.result.email == 'test_success@example.com'
    assert response.error == ''

    events_in_outbox = EventOutbox.objects.all()
    assert events_in_outbox.count() == 1
    event = events_in_outbox.first()
    assert event.event_type == 'user_created'
    assert event.event_context['email'] == 'test_success@example.com'
    assert event.event_context['first_name'] == 'Test'
    assert event.event_context['last_name'] == 'Testovich'
