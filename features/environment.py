import os

from django.db import transaction
from django.conf import settings
from app.models import Movie, FlattenedMovie


def before_all(context):
    os.environ['TMDB_API'] = 'test'
    settings.CELERY_TASK_EAGER_PROPAGATES = 'True'
    settings.CELERY_TASK_ALWAYS_EAGER = 'True'
    settings.CELERY_BROKER_URL = 'memory://'
    os.environ['CELERY_BROKER_URL'] = 'memory://'


def after_scenario(context, feature):
    if hasattr(context, 'mocker'):
        context.mocker.stop()
    with transaction.atomic():
        Movie.objects.all().delete()
        FlattenedMovie.objects.all().delete()
