from django.db import migrations, models


def backfill_cancellation_fields(apps, schema_editor):
    UserSubscription = apps.get_model("billing", "UserSubscription")

    for subscription in UserSubscription.objects.all():
        payload = subscription.raw_payload_json or {}
        details = payload.get("cancellation_details") or {}

        reason = str(details.get("reason") or "").strip()
        feedback = str(details.get("feedback") or "").strip()
        comment = str(details.get("comment") or "").strip()

        parts = []
        if reason:
            parts.append(f"reason={reason}")
        if feedback:
            parts.append(f"feedback={feedback}")
        if comment and comment != ".":
            parts.append(f"comment={comment}")

        subscription.cancel_reason = "; ".join(parts)
        subscription.cancellation_feedback = feedback
        subscription.cancellation_comment = comment
        subscription.save(
            update_fields=[
                "cancel_reason",
                "cancellation_feedback",
                "cancellation_comment",
                "updated_at",
            ]
        )


class Migration(migrations.Migration):

    dependencies = [
        ("billing", "0003_planprice_explicit_fields"),
    ]

    operations = [
        migrations.AddField(
            model_name="usersubscription",
            name="cancel_reason",
            field=models.TextField(blank=True, default=""),
        ),
        migrations.AddField(
            model_name="usersubscription",
            name="cancellation_feedback",
            field=models.TextField(blank=True, default=""),
        ),
        migrations.AddField(
            model_name="usersubscription",
            name="cancellation_comment",
            field=models.TextField(blank=True, default=""),
        ),
        migrations.RunPython(
            backfill_cancellation_fields,
            migrations.RunPython.noop,
        ),
    ]
