# Generated manually to match the current project structure.

import django.db.models.deletion
from django.conf import settings
from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name="SubscriptionPlan",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("key", models.CharField(max_length=50, unique=True)),
                ("name", models.CharField(max_length=100)),
                ("description", models.TextField(blank=True, default="")),
                ("active_alert_limit", models.PositiveIntegerField(default=0)),
                ("is_active", models.BooleanField(default=True)),
                ("features_json", models.JSONField(blank=True, default=dict)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "ordering": ["name"],
            },
        ),
        migrations.CreateModel(
            name="WebhookEvent",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("provider", models.CharField(default="stripe", max_length=30)),
                ("event_id", models.CharField(max_length=100, unique=True)),
                ("event_type", models.CharField(max_length=100)),
                ("livemode", models.BooleanField(default=False)),
                ("processed_at", models.DateTimeField(blank=True, null=True)),
                ("payload_json", models.JSONField(blank=True, default=dict)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
            ],
            options={
                "ordering": ["-created_at"],
            },
        ),
        migrations.CreateModel(
            name="PlanPrice",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("stripe_product_id", models.CharField(blank=True, default="", max_length=100)),
                ("stripe_price_id", models.CharField(max_length=100, unique=True)),
                (
                    "interval",
                    models.CharField(
                        choices=[
                            ("month", "Monthly"),
                            ("year", "Yearly"),
                            ("one_time", "One time"),
                        ],
                        default="month",
                        max_length=20,
                    ),
                ),
                ("is_active", models.BooleanField(default=True)),
                ("metadata_json", models.JSONField(blank=True, default=dict)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "plan",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="prices",
                        to="billing.subscriptionplan",
                    ),
                ),
            ],
            options={
                "ordering": ["plan__name", "interval", "stripe_price_id"],
            },
        ),
        migrations.CreateModel(
            name="UserSubscription",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                (
                    "status",
                    models.CharField(
                        choices=[
                            ("incomplete", "Incomplete"),
                            ("incomplete_expired", "Incomplete expired"),
                            ("trialing", "Trialing"),
                            ("active", "Active"),
                            ("past_due", "Past due"),
                            ("canceled", "Canceled"),
                            ("unpaid", "Unpaid"),
                        ],
                        default="active",
                        max_length=30,
                    ),
                ),
                ("stripe_customer_id", models.CharField(blank=True, default="", max_length=100)),
                (
                    "stripe_subscription_id",
                    models.CharField(blank=True, default="", max_length=100),
                ),
                ("current_period_start", models.DateTimeField(blank=True, null=True)),
                ("current_period_end", models.DateTimeField(blank=True, null=True)),
                ("cancel_at_period_end", models.BooleanField(default=False)),
                ("raw_payload_json", models.JSONField(blank=True, default=dict)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "plan",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.PROTECT,
                        related_name="subscriptions",
                        to="billing.subscriptionplan",
                    ),
                ),
                (
                    "user",
                    models.OneToOneField(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="billing_subscription",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
            ],
            options={
                "ordering": ["user__username"],
            },
        ),
        migrations.AddIndex(
            model_name="webhookevent",
            index=models.Index(
                fields=["provider", "event_type"],
                name="billing_web_provider_cf5f4e_idx",
            ),
        ),
        migrations.AddIndex(
            model_name="webhookevent",
            index=models.Index(fields=["created_at"], name="billing_web_created_b9640f_idx"),
        ),
        migrations.AddIndex(
            model_name="planprice",
            index=models.Index(fields=["plan", "is_active"], name="billing_pla_plan_id_f8157f_idx"),
        ),
        migrations.AddIndex(
            model_name="planprice",
            index=models.Index(
                fields=["stripe_price_id"],
                name="billing_pla_stripe__8dba64_idx",
            ),
        ),
        migrations.AddIndex(
            model_name="usersubscription",
            index=models.Index(fields=["plan", "status"], name="billing_use_plan_id_4b2109_idx"),
        ),
        migrations.AddIndex(
            model_name="usersubscription",
            index=models.Index(
                fields=["stripe_customer_id"],
                name="billing_use_stripe__ef7201_idx",
            ),
        ),
        migrations.AddIndex(
            model_name="usersubscription",
            index=models.Index(
                fields=["stripe_subscription_id"],
                name="billing_use_stripe__9f36f6_idx",
            ),
        ),
    ]
