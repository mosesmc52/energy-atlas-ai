from django.db import migrations


def seed_default_plans(apps, schema_editor):
    SubscriptionPlan = apps.get_model("billing", "SubscriptionPlan")

    plans = [
        {
            "key": "free",
            "name": "Free",
            "description": "Starter plan with a small number of active alerts.",
            "active_alert_limit": 3,
        },
        {
            "key": "pro",
            "name": "Pro",
            "description": "Paid individual plan with more active alerts.",
            "active_alert_limit": 25,
        },
        {
            "key": "team",
            "name": "Team",
            "description": "Higher-capacity plan intended for shared operational use.",
            "active_alert_limit": 100,
        },
    ]

    for plan in plans:
        SubscriptionPlan.objects.update_or_create(
            key=plan["key"],
            defaults=plan,
        )


def unseed_default_plans(apps, schema_editor):
    SubscriptionPlan = apps.get_model("billing", "SubscriptionPlan")
    SubscriptionPlan.objects.filter(key__in=["free", "pro", "team"]).delete()


class Migration(migrations.Migration):

    dependencies = [
        ("billing", "0001_initial"),
    ]

    operations = [
        migrations.RunPython(seed_default_plans, unseed_default_plans),
    ]
