# Zoom Review Freeze

TrainerMate is still under Zoom Marketplace review. Keep the submitted review surface stable until Zoom approves the app or asks for a specific change.

## Frozen Surface

Do not change these without a deliberate review note:

- `app.py`
- `Procfile`
- `render.yaml`
- Zoom OAuth redirect flow
- Zoom scopes
- Zoom Marketplace app settings
- reviewer URL / hosted callback behaviour
- public privacy, terms, support and test-plan pages used in the submission

Current Render start command must remain:

```text
gunicorn app:app
```

## Safe Work During Freeze

Safe work includes:

- documentation
- Supabase migration preparation
- control API source preparation in `main.py`
- password reset/token flow source, as long as it is not made the Render start target
- desktop onboarding planning
- test/verification scripts

## Before Any Push

Run:

```powershell
python verify_zoom_review_freeze.py
python verify_trainermate.py
```

## If Zoom Requests Changes

Make the smallest possible change, document it, and add a note in Zoom Marketplace reviewer notes. Avoid broad refactors until after approval.

