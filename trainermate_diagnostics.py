import os


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


def support_summary_lines(*, identity, plan_label, build_label, status, last_sync, providers, zoom_accounts):
    return [
        f"NDORS: {identity.get('ndors') or 'Not saved'}",
        f"Email: {identity.get('email') or 'Not saved'}",
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
