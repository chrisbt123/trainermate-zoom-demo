import unittest
from unittest.mock import patch

from fastapi import HTTPException

import main


class ZoomReturnUrlTests(unittest.TestCase):
    def test_frozen_callback_is_accepted_by_oauth_start(self):
        payload = main.ZoomOAuthStartRequest(
            ndors_trainer_id="TEST123",
            device_id="focused-test-device",
            state="tmrelay:focused-test-state",
            return_url="http://127.0.0.1:8421/zoom/callback",
        )

        with patch.object(main, "ZOOM_CLIENT_ID", "test-client"), \
             patch.object(main, "ZOOM_CLIENT_SECRET", "test-secret"), \
             patch.object(main, "ZOOM_REDIRECT_URI", "https://example.test/zoom/callback"), \
             patch.object(main, "STATE_SECRET", "focused-test-state-secret"):
            response = main.zoom_oauth_start(payload)

        self.assertTrue(response["ok"])
        self.assertTrue(response["authorize_url"].startswith("https://zoom.us/oauth/authorize?"))

    def test_existing_standalone_and_dev_callbacks_remain_accepted(self):
        self.assertEqual(
            main.safe_local_return_url("http://127.0.0.1:5000/zoom/callback"),
            "http://127.0.0.1:5000/zoom/callback",
        )
        self.assertEqual(
            main.safe_local_return_url("http://localhost:5000/zoom/callback"),
            "http://localhost:5000/zoom/callback",
        )

    def test_unapproved_return_urls_are_rejected(self):
        invalid_urls = (
            "https://example.com/zoom/callback",
            "http://127.0.0.1:8421/wrong-path",
            "http://127.0.0.1:8422/zoom/callback",
            "not-a-url",
        )

        for return_url in invalid_urls:
            with self.subTest(return_url=return_url):
                with self.assertRaises(HTTPException) as raised:
                    main.safe_local_return_url(return_url)
                self.assertEqual(raised.exception.status_code, 400)


if __name__ == "__main__":
    unittest.main()
