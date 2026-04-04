from __future__ import annotations

from billing.models import SubscriptionPlan, SubscriptionStatus, UserSubscription


ENTITLED_SUBSCRIPTION_STATUSES = {
    SubscriptionStatus.ACTIVE,
    SubscriptionStatus.TRIALING,
}
DEFAULT_FREE_PLAN_KEY = "free"


def get_active_subscription(user) -> UserSubscription | None:
    subscription = getattr(user, "billing_subscription", None)
    if subscription is None:
        return None
    if subscription.status not in ENTITLED_SUBSCRIPTION_STATUSES:
        return None
    return subscription


def get_default_plan() -> SubscriptionPlan | None:
    return SubscriptionPlan.objects.filter(key=DEFAULT_FREE_PLAN_KEY, is_active=True).first()


def get_user_plan(user) -> SubscriptionPlan | None:
    subscription = get_active_subscription(user)
    if subscription is not None:
        return subscription.plan
    return get_default_plan()


def get_active_alert_limit(user) -> int:
    plan = get_user_plan(user)
    if plan is None:
        return 0
    return plan.active_alert_limit


def can_create_alert(user, current_active_alert_count: int) -> tuple[bool, str | None]:
    plan = get_user_plan(user)
    if plan is None:
        return False, "No default subscription plan is configured."

    if current_active_alert_count >= plan.active_alert_limit:
        return (
            False,
            f"Your current plan allows up to {plan.active_alert_limit} active alerts.",
        )

    return True, None
