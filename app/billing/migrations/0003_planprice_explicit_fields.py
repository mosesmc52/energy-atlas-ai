from django.db import migrations, models


def backfill_planprice_fields(apps, schema_editor):
    PlanPrice = apps.get_model("billing", "PlanPrice")
    for plan_price in PlanPrice.objects.all():
        metadata = plan_price.metadata_json or {}

        if not plan_price.display_price:
            plan_price.display_price = str(metadata.get("display_amount") or "").strip()

        if plan_price.unit_amount_cents is None:
            unit_amount = metadata.get("unit_amount")
            try:
                if unit_amount not in (None, ""):
                    plan_price.unit_amount_cents = int(unit_amount)
            except (TypeError, ValueError):
                plan_price.unit_amount_cents = None

        if not plan_price.currency:
            plan_price.currency = str(metadata.get("currency") or "usd").strip() or "usd"

        plan_price.save(
            update_fields=[
                "display_price",
                "unit_amount_cents",
                "currency",
                "updated_at",
            ]
        )


class Migration(migrations.Migration):

    dependencies = [
        ("billing", "0002_seed_default_plans"),
    ]

    operations = [
        migrations.AddField(
            model_name="planprice",
            name="currency",
            field=models.CharField(blank=True, default="usd", max_length=10),
        ),
        migrations.AddField(
            model_name="planprice",
            name="display_price",
            field=models.CharField(blank=True, default="", max_length=50),
        ),
        migrations.AddField(
            model_name="planprice",
            name="unit_amount_cents",
            field=models.PositiveIntegerField(blank=True, null=True),
        ),
        migrations.RunPython(backfill_planprice_fields, migrations.RunPython.noop),
    ]
