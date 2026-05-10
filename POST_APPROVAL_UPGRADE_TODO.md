# TrainerMate Post-Zoom-Approval Upgrade Todo

This is the "do upgrade" checklist for the moment Zoom Marketplace approval is confirmed.

Until Zoom approval is confirmed, do not change:

- `app.py`
- `Procfile`
- `render.yaml`
- Zoom app callback URLs, scopes, or marketplace legal/review URLs

## One-Line Goal

Preserve the exact Zoom-approved demo, then switch the live Render service from the review demo to the real TrainerMate control API in `main.py`, test the live API, and point the desktop app at it.

## Before Running The Upgrade

- Zoom Marketplace app status shows approved.
- GitHub repo `chrisbt123/trainermate-zoom-demo` is up to date.
- Render env vars are set on the service that will run the real API:
  - `SUPABASE_URL`
  - `SUPABASE_SERVICE_ROLE_KEY`
  - `TRAINERMATE_ADMIN_PASSWORD`
  - `RESEND_API_KEY`
  - `RESEND_FROM_EMAIL`
  - `RESEND_FROM_NAME`
  - Zoom client env vars if `main.py` is handling live Zoom callbacks.
- Resend sender/domain is verified.
- Supabase has been upgraded with `trainermate_supabase_upgrade.sql`.
- You are ready for Render to redeploy.

## Command To Run After Approval

From this repo folder:

```powershell
.\do_upgrade_after_zoom_approval.ps1 -ApprovalConfirmed -ApplyDeploymentSwitch -CommitAndPush
```

If you want Codex to do it, type:

```text
do upgrade
```

Codex should then run the script, check the diff, commit, push, and help test the live service.

## What The Upgrade Script Does

- Confirms the repo is clean before changing anything.
- Runs `verify_zoom_review_freeze.py`.
- Creates a tag for the exact approved code, named like `zoom-approved-2026-05-10`.
- Runs Python compile checks.
- Runs `verify_trainermate.py`.
- Switches `Procfile` from:

```text
web: gunicorn app:app
```

to:

```text
web: uvicorn main:app --host 0.0.0.0 --port $PORT
```

- Switches `render.yaml` from:

```text
startCommand: gunicorn app:app
```

to:

```text
startCommand: uvicorn main:app --host 0.0.0.0 --port $PORT
```

- Turns off reviewer-only mode in `render.yaml`.
- Keeps a timestamped backup of the previous `Procfile` and `render.yaml` under `.upgrade-backups/`.
- Re-runs checks after the switch.
- Shows the exact files changed.
- If `-CommitAndPush` is supplied, commits the deployment switch, pushes `main`, and pushes the Zoom approval tag.

## Manual Checks Immediately After Render Deploys

Open the live HTTPS Render URL and check:

- `/health`
- `/admin`
- `/admin/api/snapshot`
- `/register-account`
- `/login-account`
- `/reset-password`
- `/confirm-password-reset`

Then test a full dummy account:

- Register a test trainer.
- Log in.
- Request password reset.
- Receive Resend email.
- Confirm password reset.
- Log in again with the new password.
- Use admin to set free/paid.
- Use admin force password reset.
- Use admin delete on the test user.

## Desktop App Switch

Once the live API is verified, point the desktop app at the live HTTPS API instead of local `127.0.0.1`.

The exact setting depends on how the packaged desktop app is being started, but the target should be the Render HTTPS URL, for example:

```text
TRAINERMATE_API_URL=https://your-render-service.onrender.com
```

Do not sync confidential local provider credentials, certificate files, or FOBS passwords to the cloud as part of this upgrade.

## Rollback Plan

If the live API switch fails:

- Revert `Procfile` and `render.yaml` from `.upgrade-backups/`.
- Commit and push the rollback.
- Render will redeploy the previous Zoom-approved demo entrypoint.
- The Git tag keeps the approved version easy to find.

## After The Upgrade Is Stable

- Rename/split repositories so the names are not confusing:
  - `trainermate-desktop`
  - `trainermate-control-api`
  - `trainermate-website`
- Keep `chrisbt123/trainermate` for the public website unless we deliberately repurpose it.
- Keep `chrisbt123/trainermate-zoom-demo` archived or renamed after the control API is moved.
