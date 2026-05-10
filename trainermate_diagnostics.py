import os
import re


def debug_tools_enabled():
    return os.getenv('TRAINERMATE_DEBUG', '0').strip().lower() in {'1', 'true', 'yes', 'on'}


def tail_log(path, max_lines=120, latest_markers=None):
    """Return the latest useful physical log lines without synthesizing state."""
    latest_markers = latest_markers or ('Sync starting', '[MAIN] Bot starting.')
    try:
        max_lines = max(1, min(int(max_lines or 120), 500))
    except Exception:
        max_lines = 120

    if not path.exists():
        return []

    try:
        with path.open('r', encoding='utf-8', errors='replace') as f:
            lines = [line.rstrip('\n') for line in f.readlines()]
    except Exception as exc:
        return [f'Could not read debug log: {exc}']

    latest_start = None
    for idx, line in enumerate(lines):
        if any(marker in line for marker in latest_markers):
            latest_start = idx
    if latest_start is not None:
        lines = lines[latest_start:]

    return lines[-max_lines:]


def mask_email(email):
    email = (email or '').strip()
    if '@' not in email:
        return email
    name, domain = email.split('@', 1)
    if len(name) <= 2:
        masked_name = name[:1] + '-' * max(0, len(name) - 1)
    else:
        masked_name = name[:2] + '-' * max(1, len(name) - 2)
    return f'{masked_name}@{domain}'


def mask_ndors(ndors):
    text = re.sub(r'\s+', '', str(ndors or '').strip())
    if not text:
        return ''
    suffix = text[-3:] if len(text) > 3 else text[-1:]
    return ('*' * max(3, len(text) - len(suffix))) + suffix


def support_summary_lines(*, identity, plan_label, build_label, status, last_sync, providers, zoom_accounts):
    return [
        f"NDORS: {mask_ndors(identity.get('ndors')) or 'Not saved'}",
        f"Email: {mask_email(identity.get('email')) or 'Not saved'}",
        f"Plan: {plan_label}",
        f"Version: {build_label}",
        f"Status: {status}",
        f"Last sync: {last_sync or 'Not run yet'}",
        f"Providers: {len(providers or [])}",
        f"Zoom accounts: {len(zoom_accounts or [])}",
    ]


def support_message_text(*, subject, identity, plan_label, build_label, status, last_sync, providers, zoom_accounts):
    lines = support_summary_lines(
        identity=identity,
        plan_label=plan_label,
        build_label=build_label,
        status=status,
        last_sync=last_sync,
        providers=providers,
        zoom_accounts=zoom_accounts,
    )
    return "Subject: " + (subject or "NDORS not saved") + "\n\nSupport summary:\n" + "\n".join(lines)
