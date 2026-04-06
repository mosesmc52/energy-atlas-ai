from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("alerts", "0001_initial"),
    ]

    operations = [
        migrations.AlterField(
            model_name="alertrule",
            name="trigger_type",
            field=models.CharField(
                choices=[
                    ("on_true_transition", "When condition becomes true"),
                    ("every_true", "Every time condition is true"),
                    ("on_false_transition", "When condition becomes false"),
                    ("every_answer", "Every evaluation returns the answer"),
                ],
                default="on_true_transition",
                max_length=30,
            ),
        ),
    ]
