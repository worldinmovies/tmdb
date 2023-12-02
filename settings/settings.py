import sys

import mongoengine
import os
import mongomock
import sentry_sdk

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROJECT_DIR = os.path.abspath(os.path.dirname(__file__))

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = os.environ.get('DJANGO_SECRET_KEY', '!xr(&l&-)*&!$kfj_&!ku#@%z8+ox4kb$y(k$nh8ur8b5wjshj')
DEBUG = False
LANGUAGE_CODE = 'en-us'
TIME_ZONE = 'Europe/Stockholm'
USE_I18N = True
USE_L10N = True
USE_TZ = True


# Application definition

INSTALLED_APPS = [
    'daphne',
    'channels',
    'health_check',
    'health_check.cache',
    'health_check.storage',
    'health_check.contrib.migrations',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'app',
    'celery',
    'corsheaders',
    'django_crontab',
]

CRONJOBS = [
    # TMDB
    ('0 9 * * *', 'app.tmdb_importer.cron_endpoint_for_checking_updateable_movies', '>> /tmp/scheduled_job.log'),
    ('0 10 * * *', 'app.tmdb_importer.base_import', '>> /tmp/scheduled_job.log'),
    ('0 */2 * * *', 'app.tmdb_importer.fetch_tmdb_data_concurrently', '>> /tmp/scheduled_job.log'),
    # IMDB
    ('0 1 * * *', 'app.imdb_importer.import_imdb_ratings', '>> /tmp/scheduled_job.log'),
    ('0 0 * * 1', 'app.imdb_importer.import_imdb_alt_titles', '>> /tmp/scheduled_job.log'),
]

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'corsheaders.middleware.CorsMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
]

CORS_ALLOW_ALL_ORIGINS = True
ALLOWED_HOSTS = ['*',]
# ALLOWED_HOSTS = ['*']
# CORS_ORIGIN_WHITELIST = (
#    'http://localhost:3000',
#    'https://webapp.localhost'
# )

# CSRF_ALLOWED_ORIGINS = ['https://worldinmovies.duckdns.org',
#                        'http://127.0.0.1:3000',
#                        'http://localhost:3000',
#                        'https://webapp.localhost']
# CSRF_TRUSTED_ORIGINS = ['https://worldinmovies.duckdns.org',
#                        'http://127.0.0.1:3000',
#                        'http://localhost:3000',
#                        'https://webapp.localhost']

environment = os.environ.get('ENVIRONMENT', 'docker')

# RABBITMQ
rabbit_url = os.environ.get('RABBITMQ_URL', 'rabbitmq')
mq_user = os.environ.get('RABBITMQ_DEFAULT_USER', 'seppa')
mq_pass = os.environ.get('RABBITMQ_DEFAULT_PASS', 'password')
CELERY_BROKER_URL = os.environ.get('RABBITMQ_URL', f"amqp://{mq_user}:{mq_pass}@{rabbit_url}")
CELERY_TIMEZONE = "Europe/Stockholm"


ROOT_URLCONF = 'settings.urls'
ASGI_APPLICATION = 'settings.asgi.application'
redis_url = 'redis' if environment == 'docker' else 'localhost'
if 'test' in sys.argv:
    CHANNEL_LAYERS = {
        "default": {
            "BACKEND": "channels.layers.InMemoryChannelLayer"
        }
    }
else:
    CHANNEL_LAYERS = {
        'default': {
            'BACKEND': "channels_redis.core.RedisChannelLayer",
            'CONFIG': {
                'hosts': [(redis_url, 6379)],
            }
        }
    }

# ---------------- MONGO -----------------

if environment == 'docker' or environment == 'localhost':
    mongo_url = 'mongo:27017'
    mongo_user = os.environ.get('MONGO_USER',       'seppa')
    mongo_pass = os.environ.get('MONGO_PASSWORD',   'password')
    mongoengine.connect(db='tmdb', host=mongo_url, port=27017, username='', password='', serverSelectionTimeoutMS=3000)
else:
    mongo_url = 'localhost:27018'
    mongoengine.connect(db='tmdb', host='mongodb://localhost', mongo_client_class=mongomock.MongoClient, username='', password='')

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': os.path.join(BASE_DIR, 'db.sqlite3')
    }
}

# Password validation
# https://docs.djangoproject.com/en/1.11/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]
# ------------------ SENTRY ------------------
sentryApi = os.getenv('SENTRY_API', '')
if sentryApi:
    sentry_sdk.init(
        dsn=sentryApi,

        # Set traces_sample_rate to 1.0 to capture 100%
        # of transactions for performance monitoring.
        # We recommend adjusting this value in production,
        traces_sample_rate=1.0,
        profiles_sample_rate=1.0,
    )

# Internationalization
# https://docs.djangoproject.com/en/1.11/topics/i18n/

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s %(levelname)s [%(name)s:%(lineno)s] %(module)s %(process)d %(thread)d %(message)s'
        }
    },
    'handlers': {
        'gunicorn': {
            'level': 'INFO',
            'class': 'logging.StreamHandler',
            'formatter': 'standard'
        },
        'console': {
            'level': 'INFO',
            'class': 'logging.StreamHandler',
            'formatter': 'standard'
        },
    },
    "root": {
        "handlers": ["console"],
        "level": "INFO",
    },
    'loggers': {
        'gunicorn.error': {
            'handlers': ['console'],
            'level': 'INFO',
            'propagate': True,
        },
        'django': {
            'handlers': ['console'],
            'level': 'INFO',
            'propagate': True,
        },
    }
}
