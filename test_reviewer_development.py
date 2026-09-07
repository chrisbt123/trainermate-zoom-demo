import ast
import os
import pathlib
import sqlite3
import tempfile
import unittest


ROOT = pathlib.Path(__file__).resolve().parent
APP = ROOT / "app.py"


def load_functions(*names, namespace=None):
    tree = ast.parse(APP.read_text(encoding="utf-8-sig"))
    wanted = set(names)
    nodes = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in wanted]
    found = {node.name for node in nodes}
    if found != wanted:
        raise AssertionError(f"Missing functions: {sorted(wanted - found)}")
    values = dict(namespace or {})
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(APP), "exec"), values)
    return values


class ReviewerDevelopmentTests(unittest.TestCase):
    def test_reviewer_password_has_no_builtin_fallback_and_fails_closed(self):
        source = APP.read_text(encoding="utf-8-sig")
        login = source[source.index("def trainer_login"):source.index("@app.route('/logout'")]
        self.assertIn("reviewer_demo_configured()", login)
        self.assertNotIn("zoomreview", login)
        helper = load_functions("reviewer_demo_configured", namespace={"REVIEWER_PASSWORD": ""})
        self.assertFalse(helper["reviewer_demo_configured"]())
        helper["reviewer_demo_configured"].__globals__["REVIEWER_PASSWORD"] = "configured-externally"
        self.assertTrue(helper["reviewer_demo_configured"]())

    def test_seeding_is_fake_and_reviewer_sync_does_not_call_fobs(self):
        source = APP.read_text(encoding="utf-8-sig")
        seed = source[source.index("def ensure_reviewer_demo_seed"):source.index("def request_wants_json")]
        sync = source[source.index("def reviewer_sync_seeded_courses"):source.index("def reviewer_seeded_sync_worker")]
        self.assertIn("essex.trainermate.local", seed)
        self.assertIn("demo_courses", seed)
        self.assertNotIn("essexfobs.co.uk", seed)
        self.assertNotIn("portal_check_and_login", sync)
        self.assertNotIn("sync_playwright", sync)

    def test_development_reviewer_blueprint_is_separate_from_production_broker(self):
        review = (ROOT / "render-review.yaml").read_text(encoding="utf-8")
        production = (ROOT / "render.yaml").read_text(encoding="utf-8")
        self.assertIn("startCommand: gunicorn app:app", review)
        self.assertIn("TRAINERMATE_REVIEWER_DEMO", review)
        self.assertIn("TRAINERMATE_REVIEWER_PASSWORD", review)
        self.assertIn("https://review.trainermate.xyz/zoom/callback", review)
        self.assertIn("startCommand: uvicorn main:app", production)
        self.assertNotIn("TRAINERMATE_REVIEWER_DEMO", production)

    def test_oauth_callback_is_environment_configured_and_hides_raw_failures(self):
        source = APP.read_text(encoding="utf-8-sig")
        config = source[source.index("_zoom_oauth_config ="):source.index("ZOOM_DEAUTHORIZATION_VERIFICATION_TOKEN")]
        callback = source[source.index("def zoom_callback"):source.index("@app.route('/zoom/set-default")]
        self.assertIn("if ISOLATED_REVIEWER_SERVICE", config)
        self.assertIn("os.getenv('ZOOM_CLIENT_ID')", config)
        self.assertIn("os.getenv('ZOOM_CLIENT_SECRET')", config)
        self.assertIn("os.getenv('TRAINERMATE_ZOOM_REDIRECT_URI')", config)
        self.assertIn("Zoom connection failed. Please return to Zoom accounts and try again.", callback)
        self.assertNotIn("response.text", callback)
        self.assertNotIn("set_flash(f'Zoom connection failed:", callback)

    def test_patch_updates_and_verifies_same_meeting_without_creation(self):
        calls = []
        saves = []

        class Response:
            def __init__(self, status_code, payload=None):
                self.status_code = status_code
                self.payload = payload or {}

            def json(self):
                return dict(self.payload)

        expected_topic = "TrainerMate - Seeded course - Updated for review"

        def request(method, url, _token, **kwargs):
            calls.append((method, url, kwargs))
            if method == "PATCH":
                return Response(204)
            return Response(200, {
                "id": 123456789,
                "topic": expected_topic,
                "join_url": "https://zoom.example/j/123456789",
                "password": "unchanged-passcode",
            })

        values = load_functions(
            "normalize_zoom_meeting_id",
            "reviewer_zoom_topic_for_course",
            "reviewer_zoom_updated_topic_for_course",
            "reviewer_patch_zoom_meeting",
            namespace={
                "re": __import__("re"),
                "reviewer_zoom_request": request,
                "reviewer_update_course_zoom": lambda *args: saves.append(args),
            },
        )
        course = {"id": "seed-1", "title": "Seeded course", "meeting_password": "unchanged-passcode"}
        ok, result = values["reviewer_patch_zoom_meeting"](course, "123 456 789", "test-token")

        self.assertTrue(ok)
        self.assertEqual(result, {"meeting_id": "123456789", "topic": expected_topic})
        self.assertEqual([call[0] for call in calls], ["PATCH", "GET"])
        self.assertEqual(calls[0][1], "https://api.zoom.us/v2/meetings/123456789")
        self.assertEqual(calls[0][2]["json"], {"topic": expected_topic})
        self.assertEqual(calls[1][1], calls[0][1])
        self.assertFalse(any(call[0] == "POST" for call in calls))
        self.assertEqual(saves[0][1], "123456789")
        self.assertEqual(saves[0][3], "unchanged-passcode")

    def test_reset_clears_seeded_course_link_without_touching_zoom_unless_requested(self):
        with tempfile.TemporaryDirectory() as directory:
            database = pathlib.Path(directory) / "courses.db"
            connection = sqlite3.connect(database)
            connection.execute(
                "CREATE TABLE courses (provider TEXT, meeting_id TEXT, meeting_link TEXT, meeting_password TEXT, "
                "last_synced_at TEXT, last_sync_status TEXT, last_sync_action TEXT)"
            )
            connection.execute(
                "INSERT INTO courses VALUES ('Essex','123456789','https://zoom.example/j/123456789','pass','now','ok','done')"
            )
            connection.commit()
            connection.close()
            zoom_saves = []
            states = []
            values = load_functions(
                "reset_reviewer_course_state",
                namespace={
                    "sqlite3": sqlite3,
                    "COURSES_DB_PATH": database,
                    "reviewer_demo_enabled": lambda: True,
                    "ensure_courses_sync_columns": lambda _connection: None,
                    "save_zoom_accounts": lambda accounts: zoom_saves.append(accounts),
                    "update_app_state": lambda **kwargs: states.append(kwargs),
                },
            )
            values["reset_reviewer_course_state"](disconnect_zoom=False)
            verify_connection = sqlite3.connect(database)
            try:
                row = verify_connection.execute(
                    "SELECT meeting_id, meeting_link, meeting_password, last_sync_status FROM courses"
                ).fetchone()
            finally:
                verify_connection.close()

        self.assertEqual(row, ("", "", "", ""))
        self.assertEqual(zoom_saves, [])
        self.assertEqual(states[0]["last_status"], "Idle")


if __name__ == "__main__":
    unittest.main()
