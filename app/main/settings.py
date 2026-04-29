from __future__ import annotations

import os
from pathlib import Path

import dj_database_url
from configurations import Configuration
from dotenv import load_dotenv
from main.sentry import init_sentry

BASE_DIR = Path(__file__).resolve().parent.parent
PROJECT_ROOT = BASE_DIR.parent

# Load environment variables for Django from the repo root first, then app/.
load_dotenv(PROJECT_ROOT / ".env")
load_dotenv(BASE_DIR / ".env")


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
        "django_ses",
        "djstripe",
        "admincolors",
        "auth.apps.AuthConfig",
        "billing.apps.BillingConfig",
        "alerts",
    ]

    ADMIN_COLORS = [("Eenrgy Atlas AI", "css/admin.css")]

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
                    "admincolors.context_processors.admin_theme",
                    "billing.context_processors.billing_subscription",
                    "main.analytics.analytics_events",
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

    STATIC_URL = "/static/"
    STATIC_ROOT = os.getenv("DJANGO_STATIC_ROOT", str(BASE_DIR / "staticfiles"))

    STATICFILES_DIRS = [BASE_DIR / "static"]
    LOGIN_URL = "/auth/signin/"
    LOGIN_REDIRECT_URL = "/alerts/"
    SECURE_PROXY_SSL_HEADER = ("HTTP_X_FORWARDED_PROTO", "https")
    EMAIL_BACKEND = "django_ses.SESBackend"
    APP_URL = os.getenv("APP_URL", "").strip()
    DEFAULT_FROM_EMAIL = os.getenv("FROM_ADDRESS", "no-reply@localhost").strip()
    SERVER_EMAIL = os.getenv("SERVER_EMAIL", DEFAULT_FROM_EMAIL).strip()
    EMAIL_SUBJECT_PREFIX = os.getenv("EMAIL_SUBJECT_PREFIX", "[Energy Atlas] ").strip()
    EMAIL_USE_SES = _get_bool("EMAIL_USE_SES", True)
    EMAIL_POSITIONS = _get_bool("EMAIL_POSITIONS", False)
    DEFAULT_NOTIFICATION_RECIPIENTS = _get_list("TO_ADDRESSES")
    AWS_SES_REGION_NAME = os.getenv("AWS_SES_REGION_NAME", "us-east-1").strip()
    AWS_SES_REGION_ENDPOINT = os.getenv(
        "AWS_SES_REGION_ENDPOINT",
        f"email.{AWS_SES_REGION_NAME}.amazonaws.com",
    ).strip()
    AWS_ACCESS_KEY_ID = os.getenv("AWS_SES_ACCESS_KEY_ID", "").strip()
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SES_SECRET_ACCESS_KEY", "").strip()
    AWS_SESSION_TOKEN = os.getenv("AWS_SES_SESSION_TOKEN", "").strip()
    AWS_SES_CONFIGURATION_SET = os.getenv("AWS_SES_CONFIGURATION_SET", "").strip()
    AWS_SES_SOURCE_ARN = os.getenv("AWS_SES_SOURCE_ARN", "").strip()
    AWS_SES_RETURN_PATH_ARN = os.getenv("AWS_SES_RETURN_PATH_ARN", "").strip()
    AWS_SES_FROM_ARN = os.getenv("AWS_SES_FROM_ARN", "").strip()
    DJSTRIPE_FOREIGN_KEY_TO_FIELD = os.getenv("DJSTRIPE_FOREIGN_KEY_TO_FIELD", "id").strip()
    GOOGLE_OAUTH_CLIENT_ID = os.getenv("GOOGLE_OAUTH_CLIENT_ID", "").strip()
    GOOGLE_OAUTH_CLIENT_SECRET = os.getenv("GOOGLE_OAUTH_CLIENT_SECRET", "").strip()
    GOOGLE_OAUTH_SCOPES = [
        "openid",
        "email",
        "profile",
    ]

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
    STRIPE_LIVE_SECRET_KEY = os.environ.get("STRIPE_LIVE_SECRET_KEY", "")
    STRIPE_TEST_SECRET_KEY = os.environ.get("STRIPE_TEST_SECRET_KEY", "")
    STRIPE_WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET", "")
    STRIPE_LIVE_MODE = False  # Change to True in production


class Production(Base):
    DEBUG = _get_bool("DJANGO_DEBUG", False)
    STRIPE_LIVE_SECRET_KEY = os.environ.get("STRIPE_LIVE_SECRET_KEY", "")
    STRIPE_TEST_SECRET_KEY = os.environ.get("STRIPE_TEST_SECRET_KEY", "")
    STRIPE_WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET", "")
    STRIPE_LIVE_MODE = _get_bool("STRIPE_LIVE_MODE", True)


init_sentry(service_name="energy-atlas-django", use_django_integration=True)
