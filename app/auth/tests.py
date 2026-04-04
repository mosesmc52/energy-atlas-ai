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
