import pytest


@pytest.fixture(scope='session')
def celery_config() -> dict:
    return {
        'broker_url': 'memory://',
        'result_backend': 'rpc://',
        'task_always_eager': True,
        'task_eager_propagates': True,
    }


@pytest.fixture(scope='session')
def celery_includes() -> list[str]:
    return ['event_outbox.tasks']
