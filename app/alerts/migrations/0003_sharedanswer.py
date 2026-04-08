from django.db import migrations, models

import alerts.models


class Migration(migrations.Migration):
    dependencies = [
        ("alerts", "0002_alertrule_every_answer_trigger"),
    ]

    operations = [
        migrations.CreateModel(
            name="SharedAnswer",
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
                    "share_id",
                    models.CharField(
                        db_index=True,
                        default=alerts.models._default_share_id,
                        max_length=32,
                        unique=True,
                    ),
                ),
                ("question", models.TextField()),
                ("response_json", models.JSONField(default=dict)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
            ],
            options={
                "ordering": ["-created_at"],
            },
        ),
    ]
