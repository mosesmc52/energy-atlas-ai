from django.contrib.auth import views as auth_views
from django.urls import path

from auth import views

app_name = "auth"

urlpatterns = [
    path("", views.sign_in_view, name="signin"),
    path("signin/", views.sign_in_view, name="signin"),
    path("google/", views.google_sign_in_start_view, name="google_start"),
    path("google/callback/", views.google_sign_in_callback_view, name="google_callback"),
    path("signup/", views.sign_up_view, name="signup"),
    path("forgot/", views.forgot_password_view, name="forgot"),
    path("logout/", auth_views.LogoutView.as_view(next_page="auth:signin"), name="logout"),
]
