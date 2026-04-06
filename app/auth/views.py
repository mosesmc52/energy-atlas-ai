from __future__ import annotations

import logging
import secrets
from urllib.parse import urlencode

import requests
from django.conf import settings
from django.contrib import messages
from django.contrib.auth import authenticate, get_user_model, login
from django.contrib.auth.decorators import login_required
from django.contrib.auth.password_validation import validate_password
from django.core.exceptions import ValidationError
from django.shortcuts import redirect, render
from django.urls import reverse
from django.views.decorators.http import require_http_methods

from main.emailing import send_templated_email

User = get_user_model()
logger = logging.getLogger(__name__)

GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_USERINFO_URL = "https://openidconnect.googleapis.com/v1/userinfo"


def _redirect_target(request) -> str:
    next_url = request.GET.get("next") or request.POST.get("next")
    return next_url or "alerts:list"


def _google_oauth_is_configured() -> bool:
    return bool(settings.GOOGLE_OAUTH_CLIENT_ID and settings.GOOGLE_OAUTH_CLIENT_SECRET)


def _google_redirect_uri(request) -> str:
    return request.build_absolute_uri(reverse("auth:google_callback"))


def _google_user_from_email(email: str):
    normalized_email = email.strip().lower()
    return User.objects.filter(username__iexact=normalized_email).first() or User.objects.filter(
        email__iexact=normalized_email
    ).first()


def _welcome_alert_url(request) -> str:
    path = reverse("alerts:create")
    app_url = str(getattr(settings, "APP_URL", "") or "").rstrip("/")
    if app_url:
        return f"{app_url}{path}"
    return request.build_absolute_uri(path)


def _send_welcome_email(*, request, user) -> None:
    recipient = str(user.email or user.username or "").strip()
    if not recipient:
        return

    try:
        send_templated_email(
            to=[recipient],
            template_base="emails/welcome",
            context={
                "create_alert_url": _welcome_alert_url(request),
            },
        )
    except Exception:  # noqa: BLE001
        logger.exception("Failed to send welcome email", extra={"user_id": user.id})


@require_http_methods(["GET"])
def google_sign_in_start_view(request):
    if request.user.is_authenticated:
        return redirect("alerts:list")

    if not _google_oauth_is_configured():
        messages.error(request, "Google sign-in is not configured yet.")
        return redirect("auth:signin")

    state = secrets.token_urlsafe(32)
    next_url = _redirect_target(request)
    request.session["google_oauth_state"] = state
    request.session["google_oauth_next"] = next_url

    query = urlencode(
        {
            "client_id": settings.GOOGLE_OAUTH_CLIENT_ID,
            "redirect_uri": _google_redirect_uri(request),
            "response_type": "code",
            "scope": " ".join(settings.GOOGLE_OAUTH_SCOPES),
            "access_type": "online",
            "include_granted_scopes": "true",
            "prompt": "select_account",
            "state": state,
        }
    )
    return redirect(f"{GOOGLE_AUTH_URL}?{query}")


@require_http_methods(["GET"])
def google_sign_in_callback_view(request):
    if not _google_oauth_is_configured():
        messages.error(request, "Google sign-in is not configured yet.")
        return redirect("auth:signin")

    expected_state = request.session.pop("google_oauth_state", "")
    next_url = request.session.pop("google_oauth_next", "alerts:list")
    returned_state = request.GET.get("state", "")
    auth_error = request.GET.get("error", "")
    code = request.GET.get("code", "")

    if auth_error:
        messages.error(request, "Google sign-in was cancelled or denied.")
        return redirect("auth:signin")

    if not expected_state or not returned_state or expected_state != returned_state:
        messages.error(request, "Google sign-in failed state validation.")
        return redirect("auth:signin")

    if not code:
        messages.error(request, "Google sign-in did not return an authorization code.")
        return redirect("auth:signin")

    token_response = requests.post(
        GOOGLE_TOKEN_URL,
        data={
            "code": code,
            "client_id": settings.GOOGLE_OAUTH_CLIENT_ID,
            "client_secret": settings.GOOGLE_OAUTH_CLIENT_SECRET,
            "redirect_uri": _google_redirect_uri(request),
            "grant_type": "authorization_code",
        },
        timeout=15,
    )
    if not token_response.ok:
        messages.error(request, "Unable to complete Google sign-in right now.")
        return redirect("auth:signin")

    access_token = str(token_response.json().get("access_token") or "").strip()
    if not access_token:
        messages.error(request, "Google sign-in did not return an access token.")
        return redirect("auth:signin")

    userinfo_response = requests.get(
        GOOGLE_USERINFO_URL,
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=15,
    )
    if not userinfo_response.ok:
        messages.error(request, "Unable to fetch your Google profile right now.")
        return redirect("auth:signin")

    profile = userinfo_response.json()
    email = str(profile.get("email") or "").strip().lower()
    email_verified = bool(profile.get("email_verified"))
    if not email:
        messages.error(request, "Google sign-in did not provide an email address.")
        return redirect("auth:signin")
    if not email_verified:
        messages.error(request, "Your Google account email must be verified to sign in.")
        return redirect("auth:signin")

    user = _google_user_from_email(email)
    created = False
    if user is None:
        user = User.objects.create_user(
            username=email,
            email=email,
            first_name=str(profile.get("given_name") or "").strip(),
            last_name=str(profile.get("family_name") or "").strip(),
        )
        created = True
    else:
        updated_fields = []
        if not user.email:
            user.email = email
            updated_fields.append("email")
        first_name = str(profile.get("given_name") or "").strip()
        last_name = str(profile.get("family_name") or "").strip()
        if first_name and not user.first_name:
            user.first_name = first_name
            updated_fields.append("first_name")
        if last_name and not user.last_name:
            user.last_name = last_name
            updated_fields.append("last_name")
        if updated_fields:
            user.save(update_fields=updated_fields)

    if created:
        _send_welcome_email(request=request, user=user)

    login(request, user)
    messages.success(request, "Signed in with Google successfully.")
    return redirect(next_url)


@require_http_methods(["GET", "POST"])
def sign_in_view(request):
    if request.user.is_authenticated:
        return redirect("alerts:list")

    if request.method == "POST":
        email = (request.POST.get("email") or "").strip().lower()
        password = request.POST.get("password") or ""

        user = authenticate(request, username=email, password=password)
        if user is None:
            messages.error(request, "Invalid email or password.")
        else:
            login(request, user)
            messages.success(request, "Signed in successfully.")
            return redirect(_redirect_target(request))

    return render(request, "auth/signin.html", {"next": request.GET.get("next", "")})


@require_http_methods(["GET", "POST"])
def sign_up_view(request):
    if request.user.is_authenticated:
        return redirect("alerts:list")

    if request.method == "POST":
        email = (request.POST.get("email") or "").strip().lower()
        password = request.POST.get("password") or ""
        confirm_password = request.POST.get("confirm_password") or ""

        if not email:
            messages.error(request, "Email is required.")
        elif password != confirm_password:
            messages.error(request, "Passwords do not match.")
        elif User.objects.filter(username=email).exists():
            messages.error(request, "An account with that email already exists.")
        else:
            try:
                validate_password(password)
            except ValidationError as exc:
                for error in exc.messages:
                    messages.error(request, error)
            else:
                user = User.objects.create_user(
                    username=email,
                    email=email,
                    password=password,
                )
                _send_welcome_email(request=request, user=user)
                login(request, user)
                messages.success(request, "Account created successfully.")
                return redirect("alerts:list")

    return render(request, "auth/signup.html")


@require_http_methods(["GET", "POST"])
def forgot_password_view(request):
    if request.method == "POST":
        email = (request.POST.get("email") or "").strip().lower()
        if email:
            user_exists = User.objects.filter(email__iexact=email).exists() or User.objects.filter(
                username__iexact=email
            ).exists()
            if user_exists:
                messages.success(
                    request,
                    "If an account exists for that email, reset instructions have been queued.",
                )
            else:
                messages.success(
                    request,
                    "If an account exists for that email, reset instructions have been queued.",
                )
            return redirect("auth:forgot")
        messages.error(request, "Enter the email associated with your account.")

    return render(request, "auth/forgot.html")
