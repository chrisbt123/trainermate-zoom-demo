# TrainerMate Zoom Marketplace Review Pack

This file is the working evidence pack for a published Zoom Marketplace OAuth app review.

## App Type

- App: TrainerMate
- Zoom integration type: OAuth user-managed app
- Redirect URI used by the desktop app: configured by `TRAINERMATE_ZOOM_REDIRECT_URI`, falling back to the value in `zoom_oauth_config.json`.
- Local callback after relay: `http://127.0.0.1:5000/zoom/callback`

For Marketplace review, the registered redirect URI should use a production-owned domain, for example `https://app.trainermate.co.uk/zoom/callback` or another stable TrainerMate domain. Avoid names that look temporary, such as `demo.*`, unless that domain is the permanent production relay and the TDD explains it.

## Required Zoom Scopes

TrainerMate needs write scope during review because the reviewer will test the Zoom update flow.

Request only the minimum scopes below:

- `user:read` or current Zoom granular equivalent for `GET /v2/users/me`
  - Used once during OAuth connect to identify the connected Zoom account email/display name.
- `meeting:read` or current granular equivalent for:
  - `GET /v2/users/me/meetings`
  - `GET /v2/meetings/{meetingId}`
  - Used to match existing scheduled meetings to course dates/titles and verify stored Zoom meeting details.
- `meeting:write` or current granular equivalent for:
  - `PATCH /v2/meetings/{meetingId}`
  - Used to update existing meeting topic/settings when a course changes or when TrainerMate must align Zoom settings with TrainerMate's saved meeting policy.

Do not request admin scopes unless the published app is explicitly account-level and admin-installed. TrainerMate's current implementation uses `me` for user-level access and should be submitted as user-level unless the product decision changes.

## Zoom API Calls Used

- `POST https://zoom.us/oauth/token`
  - Authorization-code exchange and refresh-token exchange.
- `GET https://api.zoom.us/v2/users/me`
  - Reads connected Zoom account identity.
- `GET https://api.zoom.us/v2/users/me/meetings`
  - Lists unexpired scheduled meetings for the connected user.
- `GET https://api.zoom.us/v2/meetings/{meetingId}`
  - Reads one meeting's join URL, meeting ID, password, topic, and start time.
- `PATCH https://api.zoom.us/v2/meetings/{meetingId}`
  - Updates meeting topic or selected settings for an existing meeting.

Meeting creation is disabled in the code by default (`ALLOW_ZOOM_CREATION = False`). If the Marketplace review will test meeting creation, enable it intentionally and add the creation use case to the scope justification.

## Zoom Data Collected

TrainerMate stores only the Zoom data needed for course sync:

- Zoom account email and nickname/display label.
- OAuth access and refresh tokens in the operating system keyring.
- Meeting ID, join URL, password/passcode, topic, and start time in the local course database where needed to match or verify a course.
- Connection status metadata such as `connected`, `needs_reconnect`, and verification timestamps.

TrainerMate does not collect recordings, chat messages, participant lists, transcripts, billing data, account-wide user lists, or analytics.

## Storage and Security

- OAuth client secret is not stored in `zoom_oauth_config.json`; new saves write it to the OS keyring.
- OAuth user tokens are stored in the OS keyring under `trainermate_zoom_oauth`.
- `zoom_oauth_config.json` stores non-secret app configuration only: client ID and redirect URI.
- The desktop dashboard binds to localhost and uses CSRF tokens for POST routes.
- Session cookies are `HttpOnly` and `SameSite=Lax`.
- Debug endpoints are gated by `TRAINERMATE_DEBUG=1`.
- Support/debug summaries redact common token/secret/password patterns before display.

## User Controls and Deauthorization

- Users connect Zoom from the TrainerMate Zoom Accounts page.
- Users can disconnect a Zoom account in TrainerMate; this removes the account record, removes linked provider references, and deletes stored access/refresh tokens from keyring.
- If Zoom revokes or rejects a refresh token, TrainerMate marks that account as needing reconnect and stops using stale tokens.
- The Marketplace listing should include a clear uninstall/deauthorization instruction:
  1. In TrainerMate, open Zoom Accounts and disconnect the Zoom account.
  2. In Zoom Marketplace, remove TrainerMate from added apps.

## Privacy Policy Text To Include

The privacy policy must state that TrainerMate uses Zoom OAuth to read and update scheduled meetings chosen by the connected Zoom user. It should specifically mention:

- Zoom account email/display name.
- Scheduled meeting metadata: meeting ID, topic, join URL, passcode, start time.
- Local storage on the user's machine and OS keyring token storage.
- Purpose: match NDORS/provider courses to Zoom meeting details and keep meeting information aligned.
- No sale of data.
- No access to recordings, chat, transcripts, or participant lists.
- How users can disconnect Zoom and request support/deletion.

## Technical Design Document Notes

Use these points in Zoom's TDD:

- Architecture: local desktop Flask dashboard + local bot process + hosted HTTPS redirect relay for OAuth callback back to localhost.
- Authentication: Zoom OAuth authorization code flow with state parameter. State must match the local session and `tmrelay:` prefix.
- Token handling: access/refresh tokens stored in OS keyring; refresh token rotation is persisted; failed refresh marks account as reconnect-required.
- Scope minimization: user-level profile read, meeting read, meeting write only. No admin scopes unless separately justified.
- Data retention: local course/meeting metadata retained until user deletes/local app data is removed; tokens removed on disconnect.
- Security controls: CSRF, localhost dashboard, no plaintext OAuth client secret in config, no debug endpoints unless enabled.
- OWASP posture: no arbitrary file upload execution; uploads restricted to certificate document extensions; CSP present; secrets redacted from support views.

## Before Submission Checklist

- Rotate the Zoom OAuth client secret in Marketplace because a previous local config file contained the old secret.
- Re-enter the rotated secret in TrainerMate Zoom setup so it is stored in keyring.
- Register a production-looking HTTPS redirect URI in Zoom Marketplace and set `TRAINERMATE_ZOOM_REDIRECT_URI` / `zoom_oauth_config.json` to the same value.
- Confirm requested scopes exactly match the app functions above.
- Add public Terms of Service, Privacy Policy, and Support URLs to the Marketplace listing.
- Use `ZOOM_DEAUTHORIZATION.md` as the basis for the public deauthorization/support page.
- Prepare screenshots or a short video showing:
  - Connect Zoom account.
  - Read/list scheduled meetings.
  - Update an existing meeting topic/settings.
  - Disconnect Zoom account.
