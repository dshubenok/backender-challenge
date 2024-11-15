from typing import Any

import structlog

from core.base_model import Model
from core.use_case import UseCase, UseCaseRequest, UseCaseResponse
from users.models import User
from event_outbox.models import EventOutbox
from django.conf import settings
from django.db import transaction

logger = structlog.get_logger(__name__)


class UserCreated(Model):
    email: str
    first_name: str
    last_name: str


class CreateUserRequest(UseCaseRequest):
    email: str
    first_name: str = ''
    last_name: str = ''


class CreateUserResponse(UseCaseResponse):
    result: User | None = None
    error: str = ''


class CreateUser(UseCase):
    def _get_context_vars(self, request: UseCaseRequest) -> dict[str, Any]:
        return {
            'email': request.email,
            'first_name': request.first_name,
            'last_name': request.last_name,
        }

    def _execute(self, request: CreateUserRequest) -> CreateUserResponse:
        logger.info('creating a new user')

        with transaction.atomic():
            user, created = User.objects.get_or_create(
                email=request.email,
                defaults={
                    'first_name': request.first_name, 'last_name': request.last_name,
                },
            )

            if created:
                logger.info('user has been created')
                self._log(user)
                return CreateUserResponse(result=user)

            logger.error('unable to create a new user')
            return CreateUserResponse(error='User with this email already exists')

    def _log(self, user: User) -> None:
        EventOutbox.objects.create(
            event_type='user_created',
            environment=settings.ENVIRONMENT,
            event_context={
                'email': user.email,
                'first_name': user.first_name,
                'last_name': user.last_name,
            },
            metadata_version=1,
        )
