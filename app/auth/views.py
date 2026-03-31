from __future__ import annotations

from django.contrib import messages
from django.contrib.auth import authenticate, get_user_model, login
from django.contrib.auth.decorators import login_required
from django.contrib.auth.password_validation import validate_password
from django.core.exceptions import ValidationError
from django.shortcuts import redirect, render
from django.views.decorators.http import require_http_methods

User = get_user_model()


def _redirect_target(request) -> str:
    next_url = request.GET.get("next") or request.POST.get("next")
    return next_url or "alerts:list"


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
