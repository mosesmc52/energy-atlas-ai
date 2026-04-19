from django.db import migrations, models


def migrate_trigger_types(apps, schema_editor):
    AlertRule = apps.get_model("alerts", "AlertRule")
    mapping = {
        "on_true_transition": "condition_true",
        "every_true": "condition_always",
        "on_false_transition": "condition_false",
        "every_answer": "return_answer",
    }
    for old_value, new_value in mapping.items():
        AlertRule.objects.filter(trigger_type=old_value).update(trigger_type=new_value)


class Migration(migrations.Migration):
    dependencies = [
        ("alerts", "0003_sharedanswer"),
    ]

    operations = [
        migrations.AddField(
            model_name="alertrule",
            name="last_condition_result",
            field=models.BooleanField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="alertrule",
            name="last_evaluated_value",
            field=models.FloatField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="alertrule",
            name="last_raw_value",
            field=models.FloatField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="alertrule",
            name="operator",
            field=models.CharField(
                choices=[
                    ("<", "<"),
                    ("<=", "<="),
                    (">", ">"),
                    (">=", ">="),
                    ("==", "=="),
                    ("crosses_above", "Crosses above"),
                    ("crosses_below", "Crosses below"),
                ],
                default=">=",
                max_length=30,
            ),
        ),
        migrations.AddField(
            model_name="alertrule",
            name="threshold",
            field=models.FloatField(default=0.0),
        ),
        migrations.AddField(
            model_name="alertrule",
            name="value_mode",
            field=models.CharField(
                choices=[("raw", "Raw value"), ("zscore", "Z-score")],
                default="raw",
                max_length=20,
            ),
        ),
        migrations.AlterField(
            model_name="alertrule",
            name="metric",
            field=models.CharField(max_length=100),
        ),
        migrations.AlterField(
            model_name="alertrule",
            name="signal_id",
            field=models.CharField(
                db_index=True,
                default="structured_condition",
                max_length=100,
            ),
        ),
        migrations.RunPython(migrate_trigger_types, migrations.RunPython.noop),
        migrations.AlterField(
            model_name="alertrule",
            name="trigger_type",
            field=models.CharField(
                choices=[
                    ("condition_true", "When condition becomes true"),
                    ("condition_always", "Every time condition is true"),
                    ("condition_false", "When condition becomes false"),
                    ("return_answer", "Every evaluation returns the answer"),
                ],
                default="condition_true",
                max_length=30,
            ),
        ),
    ]
