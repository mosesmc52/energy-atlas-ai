from django.contrib.auth import get_user_model
from django.core import mail
from django.test import TestCase, override_settings
from django.urls import reverse


class GoogleSignInTests(TestCase):
    def test_google_start_redirects_to_signin_when_unconfigured(self):
        response = self.client.get(reverse("auth:google_start"))

        self.assertRedirects(response, reverse("auth:signin"))

    @override_settings(
        GOOGLE_OAUTH_CLIENT_ID="client-id",
        GOOGLE_OAUTH_CLIENT_SECRET="client-secret",
    )
    def test_google_start_redirects_to_google_with_state(self):
        response = self.client.get(reverse("auth:google_start"))

        self.assertEqual(response.status_code, 302)
        self.assertIn("accounts.google.com/o/oauth2/v2/auth", response["Location"])
        session = self.client.session
        self.assertTrue(session.get("google_oauth_state"))


@override_settings(
    EMAIL_BACKEND="django.core.mail.backends.locmem.EmailBackend",
    APP_URL="https://app.energyatlas.example",
)
class SignUpWelcomeEmailTests(TestCase):
    def test_signup_sends_welcome_email(self):
        response = self.client.post(
            reverse("auth:signup"),
            data={
                "email": "newuser@example.com",
                "password": "StrongPass123!",
                "confirm_password": "StrongPass123!",
            },
        )

        self.assertRedirects(response, reverse("alerts:list"))
        self.assertEqual(len(mail.outbox), 1)
        message = mail.outbox[0]
        self.assertEqual(
            message.subject,
            "Welcome to Energy Atlas — start tracking your first signal",
        )
        self.assertEqual(message.to, ["newuser@example.com"])
        self.assertIn("Welcome to Energy Atlas.", message.body)
        self.assertIn(
            "[Create your first alert] https://app.energyatlas.example/alerts/create/",
            message.body,
        )


class AuthStatusViewTests(TestCase):
    def test_auth_status_returns_not_authenticated_for_anonymous_user(self):
        response = self.client.get(reverse("auth:status"))

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json(),
            {
                "authenticated": False,
                "email": "",
            },
        )

    def test_auth_status_returns_authenticated_user_email(self):
        user = get_user_model().objects.create_user(
            username="signedin@example.com",
            email="signedin@example.com",
            password="secret123",
        )
        self.client.force_login(user)

        response = self.client.get(reverse("auth:status"))

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json(),
            {
                "authenticated": True,
                "email": "signedin@example.com",
            },
        )
