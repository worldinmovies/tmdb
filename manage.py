#!/usr/bin/env python3
import os
import sys

from testcontainers.mongodb import MongoDbContainer

if 'test' or 'behave' in sys.argv:
    os.environ["ENVIRONMENT"] = "test"
    mongo_container = MongoDbContainer("mongo:7-jammy", dbname="test")
    mongo_container.start()
    os.environ['MONGO_URL'] = mongo_container.get_connection_url()


if __name__ == "__main__":
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "settings.settings")
    try:
        from django.core.management import execute_from_command_line
    except ImportError:
        # The above import may fail for some other reason. Ensure that the
        # issue is really that Django is missing to avoid masking other
        # exceptions on Python 2.
        try:
            import django
        except ImportError:
            raise ImportError(
                "Couldn't import Django. Are you sure it's installed and "
                "available on your PYTHONPATH environment variable? Did you "
                "forget to activate a virtual environment?"
            )
        raise
    execute_from_command_line(sys.argv)
