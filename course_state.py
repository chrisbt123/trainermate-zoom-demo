import json
import os
import tempfile
import threading
from datetime import datetime
from pathlib import Path

import trainermate_profiles

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = trainermate_profiles.data_dir_from_env(BASE_DIR)
STATE_PATH = DATA_DIR / "app_state.json"
_LOCK = threading.Lock()


def _now_utc():
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


def load_state():
    if not STATE_PATH.exists():
        return {}
    try:
        with STATE_PATH.open("r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def save_state(state):
    with _LOCK:
        state["updated_at"] = _now_utc()
        fd, tmp_name = tempfile.mkstemp(prefix=STATE_PATH.name + ".", dir=str(STATE_PATH.parent))
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(state, f, indent=2, sort_keys=True)
            os.replace(tmp_name, STATE_PATH)
        finally:
            try:
                if os.path.exists(tmp_name):
                    os.unlink(tmp_name)
            except Exception:
                pass


def update_root(**kwargs):
    state = load_state()
    for key, value in kwargs.items():
        if value is not None:
            state[key] = value
    save_state(state)
    return state


def ensure_course(course_key):
    state = load_state()
    courses = state.setdefault("courses", {})
    if course_key not in courses:
        courses[course_key] = {
            "status": "idle",
            "step": "",
            "last_action": "",
            "error": "",
            "updated_at": "",
        }
        save_state(state)


def update_course(course_key, **kwargs):
    state = load_state()
    courses = state.setdefault("courses", {})
    course = courses.setdefault(course_key, {
        "status": "idle",
        "step": "",
        "last_action": "",
        "error": "",
        "updated_at": "",
    })
    for key, value in kwargs.items():
        if value is not None:
            course[key] = value
    course["updated_at"] = _now_utc()
    state["current_course"] = course_key
    save_state(state)


def clear_courses():
    state = load_state()
    state["courses"] = {}
    state.pop("current_course", None)
    save_state(state)


def mark_running(course_key, step, msg):
    update_course(course_key, status="running", step=step, last_action=msg, error="")


def mark_success(course_key, msg="Done"):
    update_course(course_key, status="success", step="done", last_action=msg, error="")


def mark_error(course_key, step, msg):
    update_course(course_key, status="error", step=step, last_action=msg, error=msg)


def mark_skipped(course_key, msg):
    update_course(course_key, status="skipped", step="skip", last_action=msg, error="")
