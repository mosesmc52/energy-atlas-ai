from __future__ import annotations

import json

from django.core.management.base import BaseCommand, CommandError
from django.db import OperationalError, ProgrammingError

from alerts.tasks import (
    CELERY_AVAILABLE,
    evaluate_due_alert_rules,
    evaluate_due_alert_rules_now,
)


class Command(BaseCommand):
    help = (
        "Evaluate due alert rules either synchronously in-process or asynchronously "
        "by enqueueing the Celery task."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--mode",
            default="sync",
            choices=["sync", "synced", "async", "asynced"],
            help="Execution mode. Use sync/synced for in-process execution or async/asynced to queue Celery tasks.",
        )
        parser.add_argument(
            "--pretty",
            action="store_true",
            help="Pretty-print the JSON result.",
        )

    def handle(self, *args, **options):
        mode = options["mode"]
        pretty = options["pretty"]

        try:
            if mode in {"sync", "synced"}:
                payload = evaluate_due_alert_rules_now()
            else:
                if not CELERY_AVAILABLE:
                    raise CommandError(
                        "Celery is not installed in the current environment. "
                        "Run `poetry install` before using async/asynced mode."
                    )
                task = evaluate_due_alert_rules.delay()
                payload = {
                    "queued": True,
                    "task_id": task.id,
                }
        except (OperationalError, ProgrammingError) as exc:
            raise CommandError(
                "Alert tables are not available. Run migrations for the alerts app first."
            ) from exc

        if pretty:
            self.stdout.write(json.dumps(payload, indent=2, default=str))
        else:
            self.stdout.write(json.dumps(payload, default=str))
