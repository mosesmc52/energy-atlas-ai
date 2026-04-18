from django.conf import settings
from django.db import models


class SubscriptionStatus(models.TextChoices):
    INCOMPLETE = "incomplete", "Incomplete"
    INCOMPLETE_EXPIRED = "incomplete_expired", "Incomplete expired"
    TRIALING = "trialing", "Trialing"
    ACTIVE = "active", "Active"
    PAST_DUE = "past_due", "Past due"
    CANCELED = "canceled", "Canceled"
    UNPAID = "unpaid", "Unpaid"


class BillingInterval(models.TextChoices):
    MONTH = "month", "Monthly"
    YEAR = "year", "Yearly"
    ONE_TIME = "one_time", "One time"


class SubscriptionPlan(models.Model):
    key = models.CharField(max_length=50, unique=True)
    name = models.CharField(max_length=100)
    description = models.TextField(blank=True, default="")
    active_alert_limit = models.PositiveIntegerField(default=0)
    is_active = models.BooleanField(default=True)
    features_json = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["name"]

    def __str__(self) -> str:
        return self.name


class PlanPrice(models.Model):
    plan = models.ForeignKey(
        SubscriptionPlan,
        on_delete=models.CASCADE,
        related_name="prices",
    )
    stripe_product_id = models.CharField(max_length=100, blank=True, default="")
    stripe_price_id = models.CharField(max_length=100, unique=True)
    unit_amount_cents = models.PositiveIntegerField(null=True, blank=True)
    currency = models.CharField(max_length=10, blank=True, default="usd")
    display_price = models.CharField(max_length=50, blank=True, default="")
    interval = models.CharField(
        max_length=20,
        choices=BillingInterval.choices,
        default=BillingInterval.MONTH,
    )
    is_active = models.BooleanField(default=True)
    metadata_json = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["plan__name", "interval", "stripe_price_id"]
        indexes = [
            models.Index(fields=["plan", "is_active"]),
            models.Index(fields=["stripe_price_id"]),
        ]

    def __str__(self) -> str:
        return f"{self.plan.key}:{self.interval}"


class UserSubscription(models.Model):
    user = models.OneToOneField(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="billing_subscription",
    )
    plan = models.ForeignKey(
        SubscriptionPlan,
        on_delete=models.PROTECT,
        related_name="subscriptions",
    )
    status = models.CharField(
        max_length=30,
        choices=SubscriptionStatus.choices,
        default=SubscriptionStatus.ACTIVE,
    )
    stripe_customer_id = models.CharField(max_length=100, blank=True, default="")
    stripe_subscription_id = models.CharField(max_length=100, blank=True, default="")
    current_period_start = models.DateTimeField(null=True, blank=True)
    current_period_end = models.DateTimeField(null=True, blank=True)
    cancel_at_period_end = models.BooleanField(default=False)
    cancel_reason = models.TextField(blank=True, default="")
    cancellation_feedback = models.TextField(blank=True, default="")
    cancellation_comment = models.TextField(blank=True, default="")
    raw_payload_json = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["user__username"]
        indexes = [
            models.Index(fields=["plan", "status"]),
            models.Index(fields=["stripe_customer_id"]),
            models.Index(fields=["stripe_subscription_id"]),
        ]

    def __str__(self) -> str:
        return f"{self.user} -> {self.plan.key} ({self.status})"


class UserAlertAccessOverride(models.Model):
    user = models.OneToOneField(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="alert_access_override",
    )
    free_full_alert_access = models.BooleanField(
        default=False,
        help_text="Allows this account to create alerts without plan-based limits.",
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["user__username"]

    def __str__(self) -> str:
        return f"{self.user} -> free_full_alert_access={self.free_full_alert_access}"


class WebhookEvent(models.Model):
    provider = models.CharField(max_length=30, default="stripe")
    event_id = models.CharField(max_length=100, unique=True)
    event_type = models.CharField(max_length=100)
    livemode = models.BooleanField(default=False)
    processed_at = models.DateTimeField(null=True, blank=True)
    payload_json = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-created_at"]
        indexes = [
            models.Index(fields=["provider", "event_type"]),
            models.Index(fields=["created_at"]),
        ]

    def __str__(self) -> str:
        return f"{self.provider}:{self.event_type}:{self.event_id}"
