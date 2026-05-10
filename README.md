# TrainerMate Zoom reviewer demo

Hosted Render app for Zoom Marketplace review.

Reviewer URL: `https://demo.trainermate.xyz`

Start command:

```text
gunicorn app:app
```

See [README_DEPLOY_ACTUAL_APP.md](README_DEPLOY_ACTUAL_APP.md) and [ZOOM_REVIEW_SECURITY.md](ZOOM_REVIEW_SECURITY.md) before submitting to Zoom.

## Current Ownership

- This repo: Zoom reviewer demo and future TrainerMate app/control-plane source.
- `chrisbt123/trainermate`: public website, legal pages and docs for `trainermate.xyz`.

During Zoom review, keep the review app frozen. See:

- [ZOOM_REVIEW_FREEZE.md](ZOOM_REVIEW_FREEZE.md)
- [HYBRID_ARCHITECTURE.md](HYBRID_ARCHITECTURE.md)
- [POST_APPROVAL_MIGRATION.md](POST_APPROVAL_MIGRATION.md)

Before pushing while Zoom review is active:

```text
python verify_zoom_review_freeze.py
python verify_trainermate.py
```
