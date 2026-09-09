import hashlib
import hmac
import json
import unittest
from unittest.mock import patch

import app as trainermate


WEBHOOK_SECRET = "test-webhook-secret"
TIMESTAMP = "1788938000"


def request_body(event, payload):
    return json.dumps(
        {"event": event, "payload": payload},
        separators=(",", ":"),
    ).encode("utf-8")


def signature(body, timestamp=TIMESTAMP):
    message = b"v0:" + timestamp.encode("utf-8") + b":" + body
    digest = hmac.new(WEBHOOK_SECRET.encode("utf-8"), message, hashlib.sha256).hexdigest()
    return "v0=" + digest


class ZoomWebhookTests(unittest.TestCase):
    def setUp(self):
        self.client = trainermate.app.test_client()

    def post(self, body, request_signature=None, timestamp=TIMESTAMP):
        headers = {}
        if timestamp is not None:
            headers["x-zm-request-timestamp"] = timestamp
        if request_signature is not None:
            headers["x-zm-signature"] = request_signature
        return self.client.post(
            "/zoom/deauthorize",
            data=body,
            content_type="application/json",
            headers=headers,
        )

    def test_valid_signature_allows_app_deauthorization(self):
        body = request_body("app_deauthorized", {"client_id": "test-client"})
        accounts = [{"id": "zoom-account"}]
        providers = [{"zoom_account_id": "zoom-account"}]

        with patch.object(trainermate, "ZOOM_WEBHOOK_SECRET_TOKEN", WEBHOOK_SECRET), \
             patch.object(trainermate, "ZOOM_CLIENT_ID", "test-client"), \
             patch.object(trainermate, "load_zoom_accounts", return_value=accounts), \
             patch.object(trainermate, "clear_zoom_tokens") as clear_tokens, \
             patch.object(trainermate, "save_zoom_accounts") as save_accounts, \
             patch.object(trainermate, "load_providers", return_value=providers), \
             patch.object(trainermate, "save_providers") as save_providers, \
             patch.object(trainermate, "update_app_state") as update_state:
            response = self.post(body, signature(body))

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["ok"], True)
        clear_tokens.assert_called_once_with("zoom-account")
        save_accounts.assert_called_once_with([])
        save_providers.assert_called_once_with([{"zoom_account_id": ""}])
        update_state.assert_called_once()

    def test_invalid_signature_rejects_altered_body_without_side_effects(self):
        signed_body = request_body("app_deauthorized", {"client_id": "test-client"})
        altered_body = request_body("app_deauthorized", {"client_id": "other-client"})

        with patch.object(trainermate, "ZOOM_WEBHOOK_SECRET_TOKEN", WEBHOOK_SECRET), \
             patch.object(trainermate, "load_zoom_accounts") as load_accounts, \
             patch.object(trainermate, "save_zoom_accounts") as save_accounts, \
             patch.object(trainermate, "update_app_state") as update_state:
            response = self.post(altered_body, signature(signed_body))

        self.assertEqual(response.status_code, 401)
        load_accounts.assert_not_called()
        save_accounts.assert_not_called()
        update_state.assert_not_called()

    def test_invalid_signature_is_rejected(self):
        body = request_body("app_deauthorized", {"client_id": "test-client"})

        with patch.object(trainermate, "ZOOM_WEBHOOK_SECRET_TOKEN", WEBHOOK_SECRET), \
             patch.object(trainermate, "load_zoom_accounts") as load_accounts:
            response = self.post(body, "v0=invalid")

        self.assertEqual(response.status_code, 401)
        load_accounts.assert_not_called()

    def test_missing_signature_headers_are_rejected_without_side_effects(self):
        body = request_body("app_deauthorized", {"client_id": "test-client"})

        for timestamp, request_signature in ((TIMESTAMP, None), (None, signature(body))):
            with self.subTest(timestamp=timestamp, request_signature=request_signature), \
                 patch.object(trainermate, "ZOOM_WEBHOOK_SECRET_TOKEN", WEBHOOK_SECRET), \
                 patch.object(trainermate, "load_zoom_accounts") as load_accounts:
                response = self.post(body, request_signature, timestamp)

            self.assertEqual(response.status_code, 401)
            load_accounts.assert_not_called()

    def test_endpoint_url_validation_returns_zoom_challenge_response(self):
        plain_token = "zoom-plain-token"
        body = request_body("endpoint.url_validation", {"plainToken": plain_token})
        expected_encrypted_token = hmac.new(
            WEBHOOK_SECRET.encode("utf-8"),
            plain_token.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

        with patch.object(trainermate, "ZOOM_WEBHOOK_SECRET_TOKEN", WEBHOOK_SECRET), \
             patch.object(trainermate, "load_zoom_accounts") as load_accounts:
            response = self.post(body, signature(body))

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json(), {
            "plainToken": plain_token,
            "encryptedToken": expected_encrypted_token,
        })
        load_accounts.assert_not_called()


if __name__ == "__main__":
    unittest.main()
