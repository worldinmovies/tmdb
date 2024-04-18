import os

from django.db import transaction
from django.conf import settings
from app.models import Movie
from behave.fixture import use_fixture
from behave import fixture


@fixture
def setup_mongo(context):
    os.environ['ENVIRONMENT'] = 'test'
    os.environ['TMDB_API'] = 'test'
    os.environ['CELERY_BROKER_URL'] = 'memory://'
    settings.CELERY_TASK_EAGER_PROPAGATES = 'True'
    settings.CELERY_TASK_ALWAYS_EAGER = 'True'
    settings.CELERY_BROKER_URL = 'memory://'

    yield context

    del os.environ['ENVIRONMENT']
    del os.environ['MONGO_URL']
    del os.environ['TMDB_API']
    del os.environ['CELERY_BROKER_URL']


def before_all(context):
    use_fixture(setup_mongo, context)


def after_scenario(context, feature):
    if hasattr(context, 'mocker'):
        context.mocker.stop()
    with transaction.atomic():
        Movie.objects.all().delete()
