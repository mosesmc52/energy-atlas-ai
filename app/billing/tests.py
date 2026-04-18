from __future__ import annotations

from django.contrib.auth import get_user_model
from django.test import TestCase

from billing.models import SubscriptionPlan, UserAlertAccessOverride
from billing.services import can_create_alert


class AlertAccessOverrideTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(
            username="alerts-user@example.com",
            email="alerts-user@example.com",
            password="secret123",
        )
        SubscriptionPlan.objects.create(
            key="free",
            name="Free",
            active_alert_limit=1,
            is_active=True,
        )

    def test_can_create_alert_blocks_when_user_hits_plan_limit_without_override(self):
        is_allowed, error = can_create_alert(
            user=self.user,
            current_active_alert_count=1,
        )

        self.assertFalse(is_allowed)
        self.assertEqual(error, "Your current plan allows up to 1 active alerts.")

    def test_can_create_alert_allows_when_override_is_enabled(self):
        UserAlertAccessOverride.objects.create(
            user=self.user,
            free_full_alert_access=True,
        )

        is_allowed, error = can_create_alert(
            user=self.user,
            current_active_alert_count=999,
        )

        self.assertTrue(is_allowed)
        self.assertIsNone(error)
