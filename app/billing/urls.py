from django.urls import path

from billing import views

app_name = "billing"

urlpatterns = [
    path("subscription/", views.manage_subscription, name="manage"),
    path("subscription/settings/", views.billing_settings, name="settings"),
    path("checkout/", views.start_checkout, name="checkout"),
    path("webhook/", views.stripe_webhook, name="webhook"),
]
