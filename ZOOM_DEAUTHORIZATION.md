# TrainerMate Zoom Deauthorization

Users can disconnect Zoom from TrainerMate at any time.

## Disconnect in TrainerMate

1. Open TrainerMate.
2. Go to Zoom Accounts.
3. Select Disconnect beside the Zoom account.

TrainerMate removes the local Zoom account record, clears any provider links to that Zoom account, and deletes stored Zoom OAuth access/refresh tokens from the operating system keyring.

## Remove in Zoom Marketplace

1. Sign in to the Zoom App Marketplace.
2. Open Manage, then Added Apps.
3. Find TrainerMate.
4. Select Remove.

If Zoom access expires or is revoked, TrainerMate marks the account as needing reconnect and stops using stale tokens.

## Data Removed Locally

Disconnecting removes OAuth tokens and the connected account record. Historical local course rows may still contain meeting metadata, such as meeting ID or join URL, until the user deletes the local TrainerMate data or the course data is overwritten by later syncs.
