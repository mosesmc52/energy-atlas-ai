from django.urls import path

from alerts import views

app_name = "alerts"

urlpatterns = [
    path("", views.alert_list_view, name="list"),
    path("new/", views.alert_create_view, name="create"),
    path("<int:alert_rule_id>/", views.alert_detail_view, name="detail"),
    path("<int:alert_rule_id>/edit/", views.alert_edit_view, name="edit"),
    path("<int:alert_rule_id>/toggle/", views.alert_toggle_view, name="toggle"),
    path("<int:alert_rule_id>/delete/", views.alert_delete_view, name="delete"),
    path("evaluate/", views.evaluate_signal_view, name="evaluate"),
    path("forecast/", views.forecast_metric_view, name="forecast"),
    path("rules/", views.create_alert_rule_view, name="create_rule"),
]
