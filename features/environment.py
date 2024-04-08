import os

from django.db import transaction
from django.conf import settings
from app.models import Movie, FlattenedMovie
from testcontainers.mongodb import MongoDbContainer


def before_all(context):
    with MongoDbContainer("mongo:7.0.7", port=29017, dbname="test") as mongo:
        os.environ['MONGO_URL'] = mongo.get_connection_client().HOST
        os.environ['MONGO_PORT'] = str(29017)
    os.environ['TMDB_API'] = 'test'
    os.environ['ENVIRONMENT'] = 'test'
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
