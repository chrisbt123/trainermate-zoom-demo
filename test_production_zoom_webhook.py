import asyncio
import hashlib
import hmac
import json
import unittest
from unittest.mock import patch

from fastapi import HTTPException, Request

import main


WEBHOOK_SECRET = "production-webhook-test-secret"
TIMESTAMP = "1788938000"


def body_for(event, payload):
    return json.dumps({"event": event, "payload": payload}, separators=(",", ":")).encode("utf-8")


def signature_for(body):
    message = b"v0:" + TIMESTAMP.encode("ascii") + b":" + body
    return "v0=" + hmac.new(WEBHOOK_SECRET.encode("utf-8"), message, hashlib.sha256).hexdigest()


def request_for(body, signature=None, timestamp=TIMESTAMP):
    headers = [(b"content-type", b"application/json")]
    if timestamp is not None:
        headers.append((b"x-zm-request-timestamp", timestamp.encode("ascii")))
    if signature is not None:
        headers.append((b"x-zm-signature", signature.encode("ascii")))
    sent = False

    async def receive():
        nonlocal sent
        if sent:
            return {"type": "http.request", "body": b"", "more_body": False}
        sent = True
        return {"type": "http.request", "body": body, "more_body": False}

    return Request({"type": "http", "method": "POST", "path": "/zoom/deauthorize", "headers": headers}, receive)


class ProductionZoomWebhookTests(unittest.TestCase):
    def call(self, body, signature=None, timestamp=TIMESTAMP):
        return asyncio.run(main.zoom_deauthorize(request_for(body, signature, timestamp)))

    def test_valid_signature_accepts_and_relays_only_affected_zoom_identity(self):
        payload = {
            "user_id": "zoom-user-a",
            "account_id": "zoom-account-a",
            "client_id": "production-client",
            "deauthorization_time": "2026-09-09T08:00:00Z",
        }
        body = body_for("app_deauthorized", payload)
        with patch.object(main, "ZOOM_WEBHOOK_SECRET_TOKEN", WEBHOOK_SECRET), patch.object(
            main, "ZOOM_CLIENT_ID", "production-client"
        ), patch.object(main, "relay_zoom_deauthorization") as relay, patch.object(
            main, "clear_pending_zoom_user", return_value=1
        ) as clear_pending:
            response = self.call(body, signature_for(body))
        self.assertTrue(response["ok"])
        relay.assert_called_once_with(payload)
        clear_pending.assert_called_once_with("zoom-user-a", "zoom-account-a")

    def test_altered_body_is_rejected_without_relay_or_cleanup(self):
        signed = body_for("app_deauthorized", {"user_id": "a", "account_id": "a", "client_id": "production-client"})
        altered = body_for("app_deauthorized", {"user_id": "b", "account_id": "b", "client_id": "production-client"})
        with patch.object(main, "ZOOM_WEBHOOK_SECRET_TOKEN", WEBHOOK_SECRET), patch.object(
            main, "relay_zoom_deauthorization"
        ) as relay, patch.object(main, "clear_pending_zoom_user") as clear_pending:
            with self.assertRaises(HTTPException) as raised:
                self.call(altered, signature_for(signed))
        self.assertEqual(raised.exception.status_code, 401)
        relay.assert_not_called()
        clear_pending.assert_not_called()

    def test_invalid_signature_is_rejected_without_side_effects(self):
        body = body_for("app_deauthorized", {"user_id": "a", "account_id": "a", "client_id": "production-client"})
        with patch.object(main, "ZOOM_WEBHOOK_SECRET_TOKEN", WEBHOOK_SECRET), patch.object(
            main, "relay_zoom_deauthorization"
        ) as relay:
            with self.assertRaises(HTTPException) as raised:
                self.call(body, "v0=invalid")
        self.assertEqual(raised.exception.status_code, 401)
        relay.assert_not_called()

    def test_missing_signature_or_timestamp_is_rejected(self):
        body = body_for("app_deauthorized", {"user_id": "a", "account_id": "a", "client_id": "production-client"})
        with patch.object(main, "ZOOM_WEBHOOK_SECRET_TOKEN", WEBHOOK_SECRET), patch.object(
            main, "relay_zoom_deauthorization"
        ) as relay:
            for signature, timestamp in ((None, TIMESTAMP), (signature_for(body), None)):
                with self.subTest(signature=signature, timestamp=timestamp), self.assertRaises(HTTPException) as raised:
                    self.call(body, signature, timestamp)
                self.assertEqual(raised.exception.status_code, 401)
        relay.assert_not_called()

    def test_endpoint_url_validation_returns_zoom_challenge(self):
        plain_token = "plain-token"
        body = body_for("endpoint.url_validation", {"plainToken": plain_token})
        expected = hmac.new(WEBHOOK_SECRET.encode(), plain_token.encode(), hashlib.sha256).hexdigest()
        with patch.object(main, "ZOOM_WEBHOOK_SECRET_TOKEN", WEBHOOK_SECRET), patch.object(
            main, "relay_zoom_deauthorization"
        ) as relay:
            response = self.call(body, signature_for(body))
        self.assertEqual(response, {"plainToken": plain_token, "encryptedToken": expected})
        relay.assert_not_called()

    def test_valid_signature_with_wrong_client_cannot_change_data(self):
        body = body_for("app_deauthorized", {"user_id": "a", "account_id": "a", "client_id": "other-client"})
        with patch.object(main, "ZOOM_WEBHOOK_SECRET_TOKEN", WEBHOOK_SECRET), patch.object(
            main, "ZOOM_CLIENT_ID", "production-client"
        ), patch.object(main, "relay_zoom_deauthorization") as relay, patch.object(
            main, "clear_pending_zoom_user"
        ) as clear_pending:
            with self.assertRaises(HTTPException) as raised:
                self.call(body, signature_for(body))
        self.assertEqual(raised.exception.status_code, 400)
        relay.assert_not_called()
        clear_pending.assert_not_called()

    def test_relay_failure_is_retryable_and_does_not_clear_pending_data_early(self):
        body = body_for("app_deauthorized", {
            "user_id": "zoom-user-a",
            "account_id": "zoom-account-a",
            "client_id": "production-client",
        })
        relay_error = HTTPException(status_code=503, detail="relay unavailable")
        with patch.object(main, "ZOOM_WEBHOOK_SECRET_TOKEN", WEBHOOK_SECRET), patch.object(
            main, "ZOOM_CLIENT_ID", "production-client"
        ), patch.object(main, "relay_zoom_deauthorization", side_effect=relay_error), patch.object(
            main, "clear_pending_zoom_user"
        ) as clear_pending:
            with self.assertRaises(HTTPException) as raised:
                self.call(body, signature_for(body))
        self.assertEqual(raised.exception.status_code, 503)
        clear_pending.assert_not_called()


if __name__ == "__main__":
    unittest.main()
