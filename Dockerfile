FROM python:3-alpine AS base

FROM base AS builder

RUN mkdir /install
WORKDIR /install

RUN \
 apk update && \
 apk add --no-cache tzdata  && \
 apk add --no-cache --virtual .build-deps gcc libffi-dev g++ snappy-dev krb5-pkinit krb5-dev krb5

COPY requirements.txt .

RUN pip install --prefix=/install --no-cache-dir -r requirements.txt && \
    apk --purge del .build-deps

FROM base

ENV ENVIRONMENT=docker
ENV PYTHONUNBUFFERED=1
ENV TZ=Europe/Stockholm
ENV PIP_DISABLE_PIP_VERSION_CHECK=1
ENV PYTHONDONTWRITEBYTECODE=1

COPY --from=builder /install /usr/local
RUN apk --no-cache add libpq curl

COPY .. /app

WORKDIR /app


ENTRYPOINT ["/bin/sh", "-c", "python manage.py crontab add && crond & hypercorn --config=gunicorn.config.py settings.asgi:application"]
