from __future__ import annotations

from email.mime.image import MIMEImage
from pathlib import Path

from django.conf import settings
from django.core.mail import EmailMultiAlternatives
from django.template.loader import render_to_string


def send_templated_email(
    *,
    to: list[str],
    template_base: str,
    context: dict,
    from_email: str | None = None,
    reply_to: list[str] | None = None,
) -> int:
    template_context = {
        "app_url": str(getattr(settings, "APP_URL", "") or "").rstrip("/"),
        **context,
    }

    subject = render_to_string(f"{template_base}_subject.txt", template_context).strip()
    text_body = render_to_string(f"{template_base}.txt", template_context)
    html_body = render_to_string(f"{template_base}.html", template_context)

    message = EmailMultiAlternatives(
        subject=subject,
        body=text_body,
        from_email=from_email or settings.DEFAULT_FROM_EMAIL,
        to=to,
        reply_to=reply_to or None,
    )
    message.attach_alternative(html_body, "text/html")
    _attach_inline_logo(message)
    return message.send()


def _attach_inline_logo(message: EmailMultiAlternatives) -> None:
    logo_path = Path(settings.BASE_DIR) / "static" / "img" / "logo-w-title.png"
    if not logo_path.exists():
        return

    logo = MIMEImage(logo_path.read_bytes())
    logo.add_header("Content-ID", "<energy-atlas-logo>")
    logo.add_header("Content-Disposition", "inline", filename="logo-w-title.png")
    message.attach(logo)
