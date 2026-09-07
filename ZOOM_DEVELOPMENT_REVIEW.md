# Zoom Marketplace Development Review

This is an isolated reviewer deployment of the existing TrainerMate reviewer dashboard. It must be deployed as a separate Render service and must not replace or share configuration with the production desktop OAuth broker.

## Service

- Reviewer URL: `https://review.trainermate.xyz`
- Zoom Development redirect URI: `https://review.trainermate.xyz/zoom/callback`
- Start command: `gunicorn app:app`
- Blueprint: `render-review.yaml`

The existing `render.yaml` remains the production desktop OAuth broker and continues to start `main:app`.

## Required environment

Set these in the isolated reviewer service only:

```text
TRAINERMATE_REVIEWER_DEMO=1
TRAINERMATE_REVIEWER_PASSWORD=<temporary reviewer password supplied privately>
TRAINERMATE_LOGIN_EMAIL=reviewer@zoom.us
ZOOM_CLIENT_ID=<Zoom Marketplace Development Client ID>
ZOOM_CLIENT_SECRET=<Zoom Marketplace Development Client Secret>
TRAINERMATE_ZOOM_REDIRECT_URI=https://review.trainermate.xyz/zoom/callback
FLASK_SECRET_KEY=<independent random value>
SESSION_COOKIE_SECURE=1
TRAINERMATE_STARTUP_CERTIFICATE_SCAN=0
TRAINERMATE_STARTUP_ZOOM_HEALTH_CHECK=0
TRAINERMATE_AUTOMATION_SCHEDULER=0
TRAINERMATE_REMOTE_ADMIN=0
```

Do not reuse production Zoom credentials, the production Flask secret, production TrainerMate data, or a production service disk. Do not commit environment values.

## Scope under review

`meeting:update:meeting`

TrainerMate uses this permission to update the title/topic of an existing Zoom meeting owned by the connected user when linked course details change.

## Reviewer steps

1. Open `https://review.trainermate.xyz` and sign in with the reviewer email and the password supplied privately in the Marketplace submission notes.
2. Confirm seeded Essex courses are visible. These are fake course records and do not contact FOBS.
3. Open **Zoom accounts**, select **Connect Zoom account**, and authorize the Development version of TrainerMate.
4. Run a seeded course action with no linked meeting to demonstrate meeting list/search, meeting read and meeting creation where needed.
5. On that now-linked course, run the existing-meeting update action again.
6. Confirm the result visibly says the existing meeting was updated and verified, shows the updated topic, shows the same Meeting ID, and says no duplicate was created.
7. Open the meeting in Zoom and confirm its topic ends with `Updated for review` while its Meeting ID is unchanged.
8. Use the dashboard reset action to restore seeded course state when required. Use the disconnect option when the Zoom connection must also be removed.
9. From **Zoom accounts**, disconnect the reviewer Zoom account when testing is complete.

Reviewer credentials belong only in Zoom Marketplace submission notes or another approved private channel.
