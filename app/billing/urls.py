from django.urls import path

from billing import views

app_name = "billing"

urlpatterns = [
    path("checkout/", views.start_checkout, name="checkout"),
]
