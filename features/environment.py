import time
from django.db import transaction

from app.models import Movie, FlattenedMovie


def after_scenario(context, feature):
    if hasattr(context, 'mocker'):
        context.mocker.stop()
    with transaction.atomic():
        Movie.objects.all().delete()
        FlattenedMovie.objects.all().delete()
