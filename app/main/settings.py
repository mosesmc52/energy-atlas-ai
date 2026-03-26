from __future__ import annotations

import os
from pathlib import Path

import dj_database_url
from configurations import Configuration

BASE_DIR = Path(__file__).resolve().parent.parent


def _get_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _get_list(name: str, default: list[str] | None = None) -> list[str]:
    value = os.getenv(name, "")
    items = [item.strip() for item in value.split(",") if item.strip()]
    if items:
        return items
    return list(default or [])


class Base(Configuration):
    BASE_DIR = BASE_DIR

    SECRET_KEY = os.getenv(
        "DJANGO_SECRET_KEY",
        "django-insecure-o09!-2zpzmusr88h#2cm_9$io^zweq6(ttsl121u+=2i@k#$!i",
    )
    DEBUG = _get_bool("DJANGO_DEBUG", False)
    ALLOWED_HOSTS = _get_list("DJANGO_ALLOWED_HOSTS", ["localhost", "127.0.0.1"])

    INSTALLED_APPS = [
        "django.contrib.admin",
        "django.contrib.auth",
        "django.contrib.contenttypes",
        "django.contrib.sessions",
        "django.contrib.messages",
        "django.contrib.staticfiles",
        "auth.apps.AuthConfig",
        "alerts",
    ]

    MIDDLEWARE = [
        "django.middleware.security.SecurityMiddleware",
        "django.contrib.sessions.middleware.SessionMiddleware",
        "django.middleware.common.CommonMiddleware",
        "django.middleware.csrf.CsrfViewMiddleware",
        "django.contrib.auth.middleware.AuthenticationMiddleware",
        "django.contrib.messages.middleware.MessageMiddleware",
        "django.middleware.clickjacking.XFrameOptionsMiddleware",
    ]

    ROOT_URLCONF = "main.urls"

    TEMPLATES = [
        {
            "BACKEND": "django.template.backends.django.DjangoTemplates",
            "DIRS": [BASE_DIR / "templates"],
            "APP_DIRS": True,
            "OPTIONS": {
                "context_processors": [
                    "django.template.context_processors.request",
                    "django.contrib.auth.context_processors.auth",
                    "django.contrib.messages.context_processors.messages",
                ],
            },
        },
    ]

    WSGI_APPLICATION = "main.wsgi.application"
    ASGI_APPLICATION = "main.asgi.application"

    DATABASES = {
        "default": dj_database_url.parse(
            os.getenv("DATABASE_URL", f"sqlite:///{BASE_DIR / 'alerts.sqlite3'}"),
            conn_max_age=int(os.getenv("DATABASE_CONN_MAX_AGE", "600")),
        )
    }

    AUTH_PASSWORD_VALIDATORS = [
        {
            "NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator",
        },
        {
            "NAME": "django.contrib.auth.password_validation.MinimumLengthValidator",
        },
        {
            "NAME": "django.contrib.auth.password_validation.CommonPasswordValidator",
        },
        {
            "NAME": "django.contrib.auth.password_validation.NumericPasswordValidator",
        },
    ]

    LANGUAGE_CODE = "en-us"
    TIME_ZONE = os.getenv("DJANGO_TIME_ZONE", "UTC")
    USE_I18N = True
    USE_TZ = True

    STATIC_URL = "static/"
    STATIC_ROOT = os.getenv("DJANGO_STATIC_ROOT", str(BASE_DIR / "staticfiles"))

    DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

    CELERY_BROKER_URL = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0")
    CELERY_RESULT_BACKEND = os.getenv(
        "CELERY_RESULT_BACKEND",
        os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0"),
    )
    CELERY_ACCEPT_CONTENT = ["json"]
    CELERY_TASK_SERIALIZER = "json"
    CELERY_RESULT_SERIALIZER = "json"
    CELERY_TIMEZONE = TIME_ZONE
    CELERY_TASK_ALWAYS_EAGER = _get_bool("CELERY_TASK_ALWAYS_EAGER", False)
    CELERY_TASK_EAGER_PROPAGATES = _get_bool(
        "CELERY_TASK_EAGER_PROPAGATES", CELERY_TASK_ALWAYS_EAGER
    )


class Development(Base):
    DEBUG = _get_bool("DJANGO_DEBUG", True)


class Production(Base):
    DEBUG = _get_bool("DJANGO_DEBUG", False)
