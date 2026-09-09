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
    def test_synced_reviewer_course_renders_verify_action(self):
        source = APP.read_text(encoding="utf-8-sig")
        table = source[source.index("<strong>Upcoming courses</strong>"):source.index("<h3>Items to check</h3>")]
        self.assertIn("reviewer_demo_mode=reviewer_demo_enabled()", source)
        self.assertIn("{% if reviewer_demo_mode %}", table)
        self.assertIn("{% if row.meeting_id %}Verify Zoom meeting{% else %}Create Zoom meeting{% endif %}", table)
        self.assertNotIn("row.is_action_needed and reviewer_demo_mode", table)

    def test_course_without_meeting_uses_create_label_not_verify_only(self):
        source = APP.read_text(encoding="utf-8-sig")
        table = source[source.index("<strong>Upcoming courses</strong>"):source.index("<h3>Items to check</h3>")]
        self.assertIn("{% else %}Create Zoom meeting{% endif %}", table)

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
        config = source[source.index("_zoom_oauth_config ="):source.index("ZOOM_WEBHOOK_SECRET_TOKEN")]
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

    def test_verify_reads_updates_and_rereads_only_saved_meeting(self):
        calls = []

        class Response:
            def __init__(self, status_code):
                self.status_code = status_code

        def request(method, url, _token, **kwargs):
            calls.append((method, url, kwargs))
            return Response(200)

        values = load_functions(
            "reviewer_verify_existing_zoom_meeting",
            namespace={
                "normalize_zoom_meeting_id": lambda value: "89603497442",
                "reviewer_zoom_access_token_or_message": lambda: ("development-token", ""),
                "reviewer_zoom_request": request,
                "reviewer_patch_zoom_meeting": lambda course, meeting_id, token: (
                    True,
                    {"meeting_id": meeting_id, "topic": "TrainerMate - Emergency First Aid at Work - Updated for review"},
                ),
            },
        )
        ok, message = values["reviewer_verify_existing_zoom_meeting"](
            {"id": "course-efaw-001", "meeting_id": "896 0349 7442"}
        )

        self.assertTrue(ok)
        self.assertEqual(calls, [("GET", "https://api.zoom.us/v2/meetings/89603497442", {})])
        self.assertIn("Same Meeting ID: 89603497442", message)
        self.assertIn("No duplicate was created", message)

    def test_verify_missing_existing_meeting_fails_without_creation(self):
        calls = []

        class Response:
            status_code = 404

        def request(method, url, _token, **kwargs):
            calls.append((method, url, kwargs))
            return Response()

        values = load_functions(
            "reviewer_verify_existing_zoom_meeting",
            namespace={
                "normalize_zoom_meeting_id": lambda value: "89603497442",
                "reviewer_zoom_access_token_or_message": lambda: ("development-token", ""),
                "reviewer_zoom_request": request,
                "reviewer_patch_zoom_meeting": lambda *args: self.fail("PATCH must not run for an inaccessible meeting"),
            },
        )
        ok, message = values["reviewer_verify_existing_zoom_meeting"](
            {"id": "course-efaw-001", "meeting_id": "89603497442"}
        )

        self.assertFalse(ok)
        self.assertEqual([call[0] for call in calls], ["GET"])
        self.assertIn("No replacement meeting was created", message)

    def test_reviewer_verify_route_keeps_authentication_and_csrf_enforcement(self):
        source = APP.read_text(encoding="utf-8-sig")
        security = source[source.index("def security_before_request"):source.index("@app.after_request")]
        route = source[source.index("@app.post('/reviewer/course/<course_id>/zoom')"):source.index("@app.route('/sync/course/<course_id>'")]
        self.assertIn("reviewer_demo_logged_in()", security)
        self.assertIn("validate_csrf()", security)
        self.assertIn("reviewer_course_by_id(course_id)", route)
        self.assertIn("reviewer_verify_existing_zoom_meeting(course)", route)
        self.assertIn("reviewer_create_zoom_meeting(course, replace=False)", route)

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
