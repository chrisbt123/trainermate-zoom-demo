# TrainerMate Hybrid Architecture

This repo currently contains the Zoom Marketplace reviewer demo and source for the planned TrainerMate control plane. Until Zoom approval is complete, do not change the live review surface unless Zoom specifically asks for it.

## Target Shape

TrainerMate should stay hybrid rather than becoming fully web-only immediately.

### Web / Control Plane

The web side should own:

- account registration and login
- password reset by emailed one-time token
- licence/free/paid/admin status
- admin console
- support messages and audit log
- device list and device health
- update prompts
- safe queued commands for the desktop app

### Desktop / Worker

The desktop app should own:

- FOBS/provider credentials
- provider browser automation
- Zoom OAuth tokens
- local certificate files and downloaded provider documents
- local calendar sync
- actual course/provider sync execution

### Sync Bridge

The desktop app should:

- heartbeat to the control plane
- poll for safe admin commands
- report command results
- report health/status/course summaries
- fetch account/licence/password state

The control plane should not store FOBS passwords, Zoom refresh tokens, or confidential certificate files unless a future encrypted backup system is deliberately designed.

## New Device Flow

When a trainer installs TrainerMate on a new computer:

1. Install TrainerMate.
2. Log in with NDORS ID and TrainerMate password.
3. Pull account/licence/status from the control plane.
4. Show a setup checklist:
   - licence active
   - email verified
   - provider credentials need reconnecting
   - Zoom needs reconnecting
   - first sync recommended
5. Reconnect providers and Zoom locally.
6. Run first sync to rebuild local course/calendar state.

## Repos

- `chrisbt123/trainermate`: public website, legal pages, docs, callback landing pages.
- `chrisbt123/trainermate-zoom-demo`: current Zoom reviewer demo and future app/control-plane source.

After Zoom approval, consider renaming `trainermate-zoom-demo` to something clearer such as `trainermate-app` or `trainermate-control`.

