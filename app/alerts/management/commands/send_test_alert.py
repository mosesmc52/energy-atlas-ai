from __future__ import annotations

import json

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from main.emailing import send_templated_email


class Command(BaseCommand):
    help = "Send a test alert email using the configured SES email backend."

    def add_arguments(self, parser):
        parser.add_argument(
            "--to",
            action="append",
            dest="to_addresses",
            default=[],
            help=(
                "Recipient email address. Repeat --to for multiple recipients. "
                "Defaults to TO_ADDRESSES from the environment."
            ),
        )
        parser.add_argument(
            "--subject",
            default="Test Alert",
            help="Email subject line.",
        )
        parser.add_argument(
            "--intro",
            default="This is a test alert from Energy Atlas.",
            help="Short intro line shown near the top of the email.",
        )
        parser.add_argument(
            "--body",
            default=(
                "This test confirms that django-ses is configured correctly and that "
                "templated alert emails can be delivered."
            ),
            help="Main body text for the email.",
        )
        parser.add_argument(
            "--cta-url",
            default="/alerts/",
            help="Optional call-to-action URL.",
        )
        parser.add_argument(
            "--cta-label",
            default="View alerts",
            help="Optional call-to-action label.",
        )
        parser.add_argument(
            "--recipient-name",
            default="",
            help="Optional recipient name for the template greeting.",
        )

    def handle(self, *args, **options):
        recipients = list(options["to_addresses"] or []) or list(
            getattr(settings, "DEFAULT_NOTIFICATION_RECIPIENTS", []) or []
        )
        recipients = [address.strip() for address in recipients if str(address).strip()]

        if not recipients:
            raise CommandError(
                "No recipients were provided. Pass --to or set TO_ADDRESSES in the environment."
            )

        cta_url = str(options["cta_url"] or "").strip()
        if cta_url.startswith("/"):
            app_url = str(getattr(settings, "APP_URL", "") or "").rstrip("/")
            cta_url = f"{app_url}{cta_url}" if app_url else cta_url

        payload = {
            "to": recipients,
            "subject": str(options["subject"]).strip(),
            "intro": str(options["intro"]).strip(),
            "body": str(options["body"]).strip(),
            "cta_url": cta_url,
            "cta_label": str(options["cta_label"]).strip(),
            "recipient_name": str(options["recipient_name"]).strip(),
        }

        sent_count = send_templated_email(
            to=recipients,
            template_base="emails/simple_notification",
            context=payload,
        )

        self.stdout.write(
            json.dumps(
                {
                    "sent": sent_count,
                    "to": recipients,
                    "subject": payload["subject"],
                    "from_email": settings.DEFAULT_FROM_EMAIL,
                }
            )
        )
