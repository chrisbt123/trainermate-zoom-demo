# TrainerMate Reviewer Demo - Fast Deploy

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
ZOOM_CLIENT_ID=your production Zoom client id
ZOOM_CLIENT_SECRET=your production Zoom client secret
ZOOM_REDIRECT_URI=https://demo.trainermate.xyz/zoom/callback
REVIEWER_PASSWORD=choose a temporary reviewer password
FLASK_SECRET_KEY=generate a long random string
SESSION_COOKIE_SECURE=1
```

Do not commit real secrets to GitHub.

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
https://demo.trainermate.xyz
```

## Important

Do not change Zoom Marketplace redirect, scopes, Client ID, or Client Secret during approval.
The Marketplace redirect must remain:

```text
https://demo.trainermate.xyz/zoom/callback
```
