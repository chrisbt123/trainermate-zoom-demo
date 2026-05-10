# Post-Approval Migration Plan

Do not run this plan until Zoom Marketplace approval is complete.

## Goal

Move TrainerMate toward a hybrid architecture where the web service is the control plane and the desktop app remains the secure worker for local/provider automation.

## Order Of Work

1. Confirm Zoom approval is complete.
2. Take a fresh backup of Render settings, Supabase schema and local desktop app.
3. Run `trainermate_supabase_upgrade.sql` in Supabase.
4. Configure email delivery on Render:
   - `RESEND_API_KEY`
   - `RESEND_FROM_EMAIL`
   - `RESEND_FROM_NAME`
5. Deploy the control API separately or change Render start command only after a rollback path exists.
6. Test:
   - login
   - self-service reset token email
   - admin force reset email
   - free/paid status change
   - admin delete double confirmation
   - desktop heartbeat and command polling
7. Add the desktop new-device setup wizard.
8. Rename/reorganise repos only after the deployment is stable.

## Rollback

Rollback should restore:

- previous Render start command
- previous deployed commit
- previous Supabase schema backup if needed
- previous desktop installer/build

## Environment Variables

Required for control API:

```text
SUPABASE_URL
SUPABASE_SERVICE_ROLE_KEY
TRAINERMATE_ADMIN_TOKEN
RESEND_API_KEY
RESEND_FROM_EMAIL
RESEND_FROM_NAME
```

Optional:

```text
TRAINERMATE_AUTH_RATE_LIMIT_WINDOW_SECONDS
TRAINERMATE_AUTH_RATE_LIMIT_MAX_ATTEMPTS
TRAINERMATE_AUTH_RESET_RATE_LIMIT_MAX_ATTEMPTS
TRAINERMATE_AUTH_ADMIN_RATE_LIMIT_MAX_ATTEMPTS
```

