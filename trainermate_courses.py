from datetime import datetime


def parse_dashboard_datetime(value):
    """Best-effort parse for dashboard timestamps."""
    text = (value or '').strip()
    if not text:
        return None
    formats = (
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d %H:%M',
        '%Y-%m-%dT%H:%M:%SZ',
        '%Y-%m-%dT%H:%M:%S',
    )
    for fmt in formats:
        try:
            return datetime.strptime(text[:len(datetime.now().strftime(fmt))] if '%f' not in fmt else text, fmt)
        except Exception:
            pass
    try:
        return datetime.fromisoformat(text.replace('Z', '+00:00')).replace(tzinfo=None)
    except Exception:
        return None


def visible_course_where_clause():
    """SQL rule for future courses that should be visible in TrainerMate.

    Keep dashboard lists, provider counts, and calendar events on this one rule
    so stale/cancelled duplicate rows do not disappear in one view but remain in
    another.
    """
    return """
              WHERE date_time >= ?
                AND COALESCE(status, '') <> 'Replaced'
                AND lower(COALESCE(last_sync_status, '')) NOT IN ('duplicate_removed', 'removed_confirmed')
                AND lower(COALESCE(last_sync_action, '')) NOT LIKE '%course replaced by provider%'
                AND lower(COALESCE(last_sync_action, '')) NOT LIKE '%duplicate resolved by trainer%'
                AND lower(COALESCE(last_sync_action, '')) NOT LIKE '%trainer confirmed removed%'
                AND (
                    COALESCE(active_in_portal, 1) = 1
                    OR COALESCE(last_sync_status, '') = 'needs_confirmation'
                    OR lower(COALESCE(last_sync_action, '')) LIKE '%possibly removed%'
                    OR lower(COALESCE(last_sync_action, '')) LIKE '%possibly cancelled%'
                    OR lower(COALESCE(last_sync_action, '')) LIKE '%not found in latest provider scan%'
                    OR COALESCE(last_synced_at, '') <> ''
                    OR COALESCE(meeting_id, '') <> ''
                    OR COALESCE(meeting_link, '') <> ''
                )"""


def suppress_stale_same_provider_slot_duplicates(rows):
    """Hide stale same-provider duplicate rows where FOBS has replaced one course with another.

    If the same provider/date/time has multiple course titles, keep the active,
    newest checked/current row. This covers provider replacements such as one
    course title being cancelled and replaced at the same slot.
    """
    grouped = {}
    for row in rows:
        key = (row.get('provider_id'), row.get('date_label'), row.get('time_label'))
        grouped.setdefault(key, []).append(row)

    filtered = []
    for group in grouped.values():
        titles = {r.get('title') for r in group}
        if len(group) <= 1 or len(titles) <= 1:
            filtered.extend(group)
            continue

        def score(row):
            checked = parse_dashboard_datetime(row.get('checked_source') or row.get('last_seen_at') or '')
            checked_ts = checked.timestamp() if checked else 0
            status_bonus = 2 if row.get('status_label') == 'Synced' else 1 if row.get('status_label') == 'Sync due' else 0
            active_bonus = 0 if str(row.get('active_in_portal', '1')).strip() == '0' else 3
            return (active_bonus, checked_ts, status_bonus)

        filtered.append(max(group, key=score))
    return filtered
