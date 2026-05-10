import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent
FREEZE_COMMIT = "75cc41a"
FROZEN_FILES = ["app.py", "Procfile", "render.yaml"]


def git_show(path):
    return subprocess.check_output(
        ["git", "show", f"{FREEZE_COMMIT}:{path}"],
        cwd=ROOT,
        text=True,
        encoding="utf-8",
    )


def read(path):
    return (ROOT / path).read_text(encoding="utf-8")


for file_name in FROZEN_FILES:
    current = read(file_name)
    frozen = git_show(file_name)
    if current != frozen:
        raise SystemExit(
            f"Zoom review freeze failed: {file_name} differs from {FREEZE_COMMIT}. "
            "Do not change review-surface files until Zoom approval is complete."
        )

render_yaml = read("render.yaml")
procfile = read("Procfile")
if "gunicorn app:app" not in render_yaml or "gunicorn app:app" not in procfile:
    raise SystemExit("Zoom review freeze failed: Render must still start gunicorn app:app")

for forbidden in ("uvicorn main:app", "gunicorn main:app", "main.py"):
    if forbidden in render_yaml or forbidden in procfile:
        raise SystemExit(f"Zoom review freeze failed: forbidden start reference found: {forbidden}")

print("OK: Zoom review surface is frozen")
