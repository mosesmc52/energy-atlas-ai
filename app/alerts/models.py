import secrets

from django.conf import settings
from django.core.validators import MinValueValidator
from django.db import models
from django.utils import timezone

# Create your models here.


class AlertFrequency(models.TextChoices):
    HOURLY = "hourly", "Hourly"
    DAILY = "daily", "Daily"
    WEEKLY = "weekly", "Weekly"


class AlertTriggerType(models.TextChoices):
    CONDITION_TRUE = "condition_true", "When condition becomes true"
    CONDITION_ALWAYS = "condition_always", "Every time condition is true"
    CONDITION_FALSE = "condition_false", "When condition becomes false"
    RETURN_ANSWER = "return_answer", "Every evaluation returns the answer"


class AlertValueMode(models.TextChoices):
    RAW = "raw", "Raw value"
    ZSCORE = "zscore", "Z-score"


class AlertOperator(models.TextChoices):
    LT = "<", "<"
    LTE = "<=", "<="
    GT = ">", ">"
    GTE = ">=", ">="
    EQ = "==", "=="
    CROSSES_ABOVE = "crosses_above", "Crosses above"
    CROSSES_BELOW = "crosses_below", "Crosses below"


class AlertDeliveryChannel(models.TextChoices):
    EMAIL = "email", "Email"
    WEBHOOK = "webhook", "Webhook"
    TELEGRAM = "telegram", "Telegram"


class AlertStatus(models.TextChoices):
    ACTIVE = "active", "Active"
    PAUSED = "paused", "Paused"
    DISABLED = "disabled", "Disabled"


class AlertRule(models.Model):
    """
    Stores a user-defined alert tied to a built-in signal template.
    The signal engine should use `signal_id` + `config_json` to evaluate it.
    """

    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="alert_rules",
    )

    name = models.CharField(max_length=255)
    question = models.CharField(max_length=500)

    # Legacy signal identifier; structured rules use "structured_condition".
    signal_id = models.CharField(
        max_length=100,
        db_index=True,
        default="structured_condition",
    )

    # Optional categorization
    metric = models.CharField(max_length=100)
    value_mode = models.CharField(
        max_length=20,
        choices=AlertValueMode.choices,
        default=AlertValueMode.RAW,
    )
    operator = models.CharField(
        max_length=30,
        choices=AlertOperator.choices,
        default=AlertOperator.GTE,
    )
    threshold = models.FloatField(default=0.0)
    region = models.CharField(max_length=100, blank=True, default="")

    # Dynamic parameters for the signal
    # Example:
    # {
    #   "threshold": -10.0,
    #   "reference": "five_year_average",
    #   "unit": "percent_diff",
    #   "lookback": "latest"
    # }
    config_json = models.JSONField(default=dict, blank=True)

    status = models.CharField(
        max_length=20,
        choices=AlertStatus.choices,
        default=AlertStatus.ACTIVE,
        db_index=True,
    )

    frequency = models.CharField(
        max_length=20,
        choices=AlertFrequency.choices,
        default=AlertFrequency.DAILY,
    )

    trigger_type = models.CharField(
        max_length=30,
        choices=AlertTriggerType.choices,
        default=AlertTriggerType.CONDITION_TRUE,
    )

    # Basic cooldown to suppress repeated sends
    cooldown_hours = models.PositiveIntegerField(
        default=24,
        validators=[MinValueValidator(0)],
    )

    # Store channels as list, e.g. ["email"]
    delivery_channels = models.JSONField(default=list, blank=True)

    is_active = models.BooleanField(default=True, db_index=True)

    # Evaluation state tracking
    last_evaluated_at = models.DateTimeField(null=True, blank=True)
    last_result = models.BooleanField(null=True, blank=True)
    last_raw_value = models.FloatField(null=True, blank=True)
    last_evaluated_value = models.FloatField(null=True, blank=True)
    last_condition_result = models.BooleanField(null=True, blank=True)
    last_triggered_at = models.DateTimeField(null=True, blank=True)
    last_notified_at = models.DateTimeField(null=True, blank=True)

    # Optional: store latest explanation/values for quick dashboard rendering
    last_explanation = models.TextField(blank=True, default="")
    last_values_json = models.JSONField(default=dict, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-created_at"]
        indexes = [
            models.Index(fields=["user", "status", "is_active"]),
            models.Index(fields=["signal_id", "is_active"]),
            models.Index(fields=["frequency", "is_active"]),
        ]

    def __str__(self) -> str:
        return f"{self.name} ({self.user})"

    @property
    def is_enabled(self) -> bool:
        return self.is_active and self.status == AlertStatus.ACTIVE

    def in_cooldown(self, now=None) -> bool:
        if not self.last_notified_at or self.cooldown_hours == 0:
            return False
        now = now or timezone.now()
        delta = now - self.last_notified_at
        return delta.total_seconds() < self.cooldown_hours * 3600


class AlertEvent(models.Model):
    """
    Stores the result of an alert evaluation.
    One row per evaluation attempt is usually best for auditability.
    """

    alert_rule = models.ForeignKey(
        AlertRule,
        on_delete=models.CASCADE,
        related_name="events",
    )

    evaluated_at = models.DateTimeField(default=timezone.now, db_index=True)

    # None means could not evaluate (missing data, adapter issue, etc.)
    result = models.BooleanField(null=True, blank=True, db_index=True)

    explanation = models.TextField(blank=True, default="")

    # Example:
    # {
    #   "current_storage": 1789,
    #   "five_year_average": 2028,
    #   "pct_diff": -11.8,
    #   "threshold": -10.0
    # }
    values_json = models.JSONField(default=dict, blank=True)

    error_code = models.CharField(max_length=100, blank=True, default="")
    error_message = models.TextField(blank=True, default="")

    was_triggered = models.BooleanField(default=False, db_index=True)
    notification_sent = models.BooleanField(default=False)
    notification_sent_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ["-evaluated_at"]
        indexes = [
            models.Index(fields=["alert_rule", "evaluated_at"]),
            models.Index(fields=["was_triggered", "evaluated_at"]),
            models.Index(fields=["result", "evaluated_at"]),
        ]

    def __str__(self) -> str:
        return f"AlertEvent(alert_rule_id={self.alert_rule_id}, result={self.result}, triggered={self.was_triggered})"


def _default_share_id() -> str:
    return secrets.token_urlsafe(9)


class SharedAnswer(models.Model):
    share_id = models.CharField(
        max_length=32,
        unique=True,
        db_index=True,
        default=_default_share_id,
    )
    question = models.TextField()
    response_json = models.JSONField(default=dict)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-created_at"]

    def __str__(self) -> str:
        return f"SharedAnswer(share_id={self.share_id})"
