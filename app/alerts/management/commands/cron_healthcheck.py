from __future__ import annotations

import json
import os
import socket
from datetime import datetime, timezone
from pathlib import Path

from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = "Write a cron heartbeat file and emit a log line so scheduler health is easy to verify."

    def add_arguments(self, parser):
        parser.add_argument(
            "--output",
            default="",
            help=(
                "Optional heartbeat output path. Defaults to "
                "data/health/cron_heartbeat.json under the repository root."
            ),
        )

    def handle(self, *args, **options):
        output_arg = str(options.get("output") or "").strip()
        output_path = self._resolve_output_path(output_arg)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        heartbeat = {
            "ok": True,
            "task": "cron_healthcheck",
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "hostname": socket.gethostname(),
            "pid": os.getpid(),
        }
        output_path.write_text(json.dumps(heartbeat, indent=2) + "\n", encoding="utf-8")

        self.stdout.write(
            f"cron_healthcheck ok timestamp={heartbeat['timestamp_utc']} output={output_path}"
        )

    def _resolve_output_path(self, output_arg: str) -> Path:
        if output_arg:
            return Path(output_arg).expanduser().resolve()

        repo_root = Path(__file__).resolve().parents[5]
        return repo_root / "data" / "health" / "cron_heartbeat.json"
