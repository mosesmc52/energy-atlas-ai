from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "app"))

from alerts.management.commands.cron_healthcheck import Command  # noqa: E402


class TestCronHealthcheckCommand(unittest.TestCase):
    def test_handle_writes_heartbeat_file(self) -> None:
        command = Command()

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "cron_heartbeat.json"
            command.handle(output=str(output_path))

            self.assertTrue(output_path.exists())
            payload = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertTrue(payload["ok"])
            self.assertEqual(payload["task"], "cron_healthcheck")
            self.assertIn("timestamp_utc", payload)
            self.assertIn("hostname", payload)
            self.assertIn("pid", payload)


if __name__ == "__main__":
    unittest.main()
