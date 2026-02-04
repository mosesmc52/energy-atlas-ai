from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

load_dotenv()

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


@dataclass(frozen=True)
class QuestionRow:
    ts_utc: str
    user_id: str
    session_id: str
    question: str
    tags: str
    metadata_json: str


class GoogleSheetsQuestionLogger:
    """
    Append question rows into a Google Sheet using env-based credentials.

    Required environment variables:
      - SERVICE_ACCOUNT_FILE=path/to/service-account.json
      - GOOGLE_SHEET_DOCUMENT_ID=<spreadsheet_id>
    """

    def __init__(
        self,
        *,
        sheet_name: str = "questions",
        service_account_file_env: str = "SERVICE_ACCOUNT_FILE",
        spreadsheet_id_env: str = "GOOGLE_SHEET_DOCUMENT_ID",
    ):
        self.sheet_name = sheet_name

        service_account_file = os.getenv(service_account_file_env)
        spreadsheet_id = os.getenv(spreadsheet_id_env)

        if not service_account_file:
            raise RuntimeError(
                f"Missing env var {service_account_file_env} "
                "(path to service-account.json)"
            )

        if not spreadsheet_id:
            raise RuntimeError(
                f"Missing env var {spreadsheet_id_env} " "(Google Sheet document ID)"
            )

        if not os.path.exists(service_account_file):
            raise RuntimeError(
                f"Service account file not found: {service_account_file}"
            )

        self.spreadsheet_id = spreadsheet_id

        creds = Credentials.from_service_account_file(
            service_account_file,
            scopes=SCOPES,
        )

        # cache_discovery=False avoids issues in containerized envs
        self.svc = build(
            "sheets",
            "v4",
            credentials=creds,
            cache_discovery=False,
        )

    # -------------------------
    # Public API
    # -------------------------

    def ensure_header(self, header: Optional[List[str]] = None) -> None:
        """
        Ensure the sheet has a header row (best-effort, idempotent).
        """
        header = header or [
            "ts_utc",
            "user_id",
            "session_id",
            "question",
            "tags",
            "metadata_json",
        ]

        rng = f"{self.sheet_name}!A1:F1"
        resp = (
            self.svc.spreadsheets()
            .values()
            .get(
                spreadsheetId=self.spreadsheet_id,
                range=rng,
            )
            .execute()
        )

        values = resp.get("values", [])
        if values and len(values[0]) > 0:
            return  # header already present

        self._append_raw_row(header)

    def append_question(
        self,
        *,
        question: str,
        user_id: str = "",
        session_id: str = "",
        tags: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        ts_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        row = QuestionRow(
            ts_utc=ts_utc,
            user_id=user_id,
            session_id=session_id,
            question=question,
            tags=",".join(tags or []),
            metadata_json=json.dumps(
                metadata or {},
                ensure_ascii=False,
                separators=(",", ":"),
            ),
        )

        self._append_raw_row(
            [
                row.ts_utc,
                row.user_id,
                row.session_id,
                row.question,
                row.tags,
                row.metadata_json,
            ]
        )

    # -------------------------
    # Internal helpers
    # -------------------------

    def _append_raw_row(self, row: List[Any]) -> None:
        body = {"values": [row]}

        (
            self.svc.spreadsheets()
            .values()
            .append(
                spreadsheetId=self.spreadsheet_id,
                range=f"{self.sheet_name}!A1",
                valueInputOption="USER_ENTERED",
                insertDataOption="INSERT_ROWS",
                body=body,
            )
            .execute()
        )
