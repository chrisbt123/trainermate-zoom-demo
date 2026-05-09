# TrainerMate Zoom Marketplace review notes

TrainerMate's hosted reviewer environment is a Render-hosted Flask app for Zoom Marketplace review. It uses seeded training course rows so reviewers can exercise the Zoom workflow without needing private FOBS/provider credentials.

## OAuth and redirect URL

- Published review redirect URL: `https://demo.trainermate.xyz/zoom/callback`
- The app reads the redirect URL from `TRAINERMATE_ZOOM_REDIRECT_URI` or `ZOOM_REDIRECT_URI`.
- The app must be served over HTTPS for Zoom OAuth review.

## Required Render secrets

Set these in Render environment variables only. Do not commit them to GitHub.

```text
TRAINERMATE_REVIEWER_DEMO=1
ZOOM_CLIENT_ID=<production Zoom client id>
ZOOM_CLIENT_SECRET=<production Zoom client secret>
ZOOM_REDIRECT_URI=https://demo.trainermate.xyz/zoom/callback
REVIEWER_PASSWORD=<temporary reviewer password>
FLASK_SECRET_KEY=<long random secret>
SESSION_COOKIE_SECURE=1
```

## Zoom scopes exercised during review

- Read the authorised user's profile.
- List the authorised user's meetings to find matching course meetings.
- Read a specific meeting by meeting ID.
- Create scheduled meetings for seeded courses when no valid matching meeting exists.
- Update existing meetings to demonstrate write permission.

## Secret handling

- `ZOOM_CLIENT_SECRET` is loaded from Render environment variables.
- The fallback local `zoom_oauth_config.json` must not contain `client_secret`.
- Local OAuth tokens are stored in runtime state only and ignored by Git.
- If a real client secret was ever committed or visible in a submitted GitHub repo, rotate it in Zoom Marketplace before resubmitting.

## Reviewer test path

1. Open `https://demo.trainermate.xyz`.
2. Log in with the reviewer password.
3. Open Zoom accounts and connect the reviewer Zoom account.
4. Run seeded course sync or use a course-level Zoom action.
5. Verify that TrainerMate lists/reads meetings, creates a meeting where needed, and updates/verifies an existing meeting.
6. Disconnect Zoom from the Zoom accounts page when finished.
