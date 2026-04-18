from django.contrib import admin

from billing.models import (
    PlanPrice,
    SubscriptionPlan,
    UserAlertAccessOverride,
    UserSubscription,
    WebhookEvent,
)


class PlanPriceInline(admin.TabularInline):
    model = PlanPrice
    extra = 0
    fields = (
        "stripe_product_id",
        "stripe_price_id",
        "unit_amount_cents",
        "currency",
        "display_price",
        "interval",
        "is_active",
        "metadata_json",
    )


@admin.register(SubscriptionPlan)
class SubscriptionPlanAdmin(admin.ModelAdmin):
    list_display = ("key", "name", "active_alert_limit", "is_active", "updated_at")
    list_filter = ("is_active",)
    search_fields = ("key", "name", "description")
    ordering = ("name",)
    inlines = (PlanPriceInline,)


@admin.register(PlanPrice)
class PlanPriceAdmin(admin.ModelAdmin):
    list_display = (
        "stripe_price_id",
        "plan",
        "display_price",
        "currency",
        "interval",
        "is_active",
        "updated_at",
    )
    list_filter = ("interval", "is_active", "plan")
    search_fields = (
        "stripe_price_id",
        "stripe_product_id",
        "display_price",
        "plan__key",
        "plan__name",
    )
    autocomplete_fields = ("plan",)
    ordering = ("plan__name", "interval", "stripe_price_id")


@admin.register(UserSubscription)
class UserSubscriptionAdmin(admin.ModelAdmin):
    list_display = (
        "user",
        "plan",
        "status",
        "current_period_end",
        "cancel_at_period_end",
        "updated_at",
    )
    list_filter = ("status", "cancel_at_period_end", "plan")
    search_fields = (
        "user__username",
        "user__email",
        "cancel_reason",
        "cancellation_feedback",
        "cancellation_comment",
        "stripe_customer_id",
        "stripe_subscription_id",
        "plan__key",
        "plan__name",
    )
    autocomplete_fields = ("user", "plan")
    list_select_related = ("user", "plan")
    readonly_fields = (
        "cancel_reason",
        "cancellation_feedback",
        "cancellation_comment",
    )
    ordering = ("user__username",)


@admin.register(WebhookEvent)
class WebhookEventAdmin(admin.ModelAdmin):
    list_display = (
        "event_id",
        "provider",
        "event_type",
        "livemode",
        "processed_at",
        "created_at",
    )
    list_filter = ("provider", "event_type", "livemode", "processed_at")
    search_fields = ("event_id", "event_type")
    readonly_fields = ("created_at",)
    ordering = ("-created_at",)


@admin.register(UserAlertAccessOverride)
class UserAlertAccessOverrideAdmin(admin.ModelAdmin):
    list_display = (
        "user",
        "free_full_alert_access",
        "updated_at",
    )
    list_filter = ("free_full_alert_access",)
    search_fields = ("user__username", "user__email")
    autocomplete_fields = ("user",)
    list_select_related = ("user",)
    ordering = ("user__username",)
