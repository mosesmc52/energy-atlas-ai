from django.contrib import admin

from alerts.models import AlertEvent, AlertRule, SharedAnswer


class AlertEventInline(admin.TabularInline):
    model = AlertEvent
    extra = 0
    fields = (
        "evaluated_at",
        "result",
        "was_triggered",
        "notification_sent",
        "notification_sent_at",
        "error_code",
    )
    readonly_fields = fields
    can_delete = False
    show_change_link = True
    ordering = ("-evaluated_at",)


@admin.register(AlertRule)
class AlertRuleAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "name",
        "user",
        "signal_id",
        "status",
        "is_active",
        "frequency",
        "trigger_type",
        "value_mode",
        "operator",
        "threshold",
        "last_result",
        "last_triggered_at",
        "created_at",
    )
    list_filter = (
        "status",
        "is_active",
        "frequency",
        "trigger_type",
        "created_at",
        "updated_at",
    )
    search_fields = (
        "name",
        "question",
        "signal_id",
        "metric",
        "region",
        "user__username",
        "user__email",
    )
    autocomplete_fields = ("user",)
    readonly_fields = (
        "created_at",
        "updated_at",
        "last_evaluated_at",
        "last_result",
        "last_triggered_at",
        "last_notified_at",
    )
    list_select_related = ("user",)
    ordering = ("-created_at",)
    inlines = (AlertEventInline,)
    fieldsets = (
        (
            "Rule",
            {
                "fields": (
                    "user",
                    "name",
                    "question",
                    "signal_id",
                    "metric",
                    "value_mode",
                    "operator",
                    "threshold",
                    "region",
                    "config_json",
                )
            },
        ),
        (
            "Delivery",
            {
                "fields": (
                    "status",
                    "is_active",
                    "frequency",
                    "trigger_type",
                    "cooldown_hours",
                    "delivery_channels",
                )
            },
        ),
        (
            "Evaluation State",
            {
                "fields": (
                    "last_evaluated_at",
                    "last_result",
                    "last_raw_value",
                    "last_evaluated_value",
                    "last_condition_result",
                    "last_triggered_at",
                    "last_notified_at",
                    "last_explanation",
                    "last_values_json",
                )
            },
        ),
        ("Timestamps", {"fields": ("created_at", "updated_at")}),
    )


@admin.register(AlertEvent)
class AlertEventAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "alert_rule",
        "evaluated_at",
        "result",
        "was_triggered",
        "notification_sent",
        "notification_sent_at",
        "error_code",
    )
    list_filter = (
        "result",
        "was_triggered",
        "notification_sent",
        "evaluated_at",
    )
    search_fields = (
        "alert_rule__name",
        "alert_rule__question",
        "alert_rule__signal_id",
        "alert_rule__user__username",
        "alert_rule__user__email",
        "error_code",
        "error_message",
    )
    autocomplete_fields = ("alert_rule",)
    readonly_fields = ("evaluated_at", "notification_sent_at")
    list_select_related = ("alert_rule", "alert_rule__user")
    ordering = ("-evaluated_at",)


@admin.register(SharedAnswer)
class SharedAnswerAdmin(admin.ModelAdmin):
    list_display = ("id", "share_id", "question", "created_at")
    search_fields = ("share_id", "question")
    readonly_fields = ("share_id", "created_at")
    ordering = ("-created_at",)
