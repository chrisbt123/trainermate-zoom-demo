# TrainerMate Development Reviewer Demo - Fast Deploy

Use the isolated `render-review.yaml` blueprint and the instructions in `ZOOM_DEVELOPMENT_REVIEW.md`. Do not replace the existing service configured by `render.yaml`; that service is the production desktop OAuth broker.

This is a standalone hosted reviewer demo for Zoom Marketplace review.

It uses:
- real Zoom OAuth and Zoom API calls
- dummy provider/course data
- a simple shared reviewer password
- no private FOBS/provider credentials

## Files

- `app.py` - Flask reviewer demo app
- `requirements.txt` - Python dependencies
- `Procfile` - Render/Heroku-style start command
- `render.yaml` - optional Render blueprint

## Required environment variables on Render

Set these in Render > Service > Environment:

```text
ZOOM_CLIENT_ID=<Zoom Marketplace Development client id>
ZOOM_CLIENT_SECRET=<Zoom Marketplace Development client secret>
TRAINERMATE_ZOOM_REDIRECT_URI=https://review.trainermate.xyz/zoom/callback
TRAINERMATE_REVIEWER_PASSWORD=<temporary reviewer password supplied privately>
FLASK_SECRET_KEY=generate a long random string
SESSION_COOKIE_SECURE=1
```

Do not commit real secrets to GitHub.
If a real Zoom client secret was ever committed or submitted in an old repo, rotate it in Zoom Marketplace and update Render with the new value before resubmitting.

## Render start command

```text
gunicorn app:app
```

## Build command

```text
pip install -r requirements.txt
```

## Health check

```text
/healthz
```

## Reviewer URL

```text
https://review.trainermate.xyz
```

## Important

Do not change Zoom Marketplace redirect, scopes, Client ID, or Client Secret during approval.
The Marketplace redirect must remain:

```text
https://review.trainermate.xyz/zoom/callback
```
