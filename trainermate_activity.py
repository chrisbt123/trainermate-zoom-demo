import json
import uuid
from datetime import datetime
from pathlib import Path

from trainermate_utils import provider_slug


BASE_DIR = Path(__file__).resolve().parent
ACTIVITY_HISTORY_PATH = BASE_DIR / 'activity_history.json'
AUTOMATION_SETTINGS_PATH = BASE_DIR / 'automation_settings.json'


def utc_now_text():
    return datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')


def load_json(path, default):
    path = Path(path)
    try:
        if path.exists():
            return json.loads(path.read_text(encoding='utf-8'))
    except Exception:
        pass
    return default


def save_json(path, data):
    path = Path(path)
    path.write_text(json.dumps(data, indent=2), encoding='utf-8')


def load_automation_settings():
    defaults = {
        'notifications_enabled': True,
        'popup_bubbles': True,
        'notify_support_messages': True,
        'notify_problems': True,
        'notify_course_changes': True,
        'notify_success_no_changes': False,
    }
    data = load_json(AUTOMATION_SETTINGS_PATH, {})
    if isinstance(data, dict):
        defaults.update(data)
    return defaults


def load_activity_history():
    data = load_json(ACTIVITY_HISTORY_PATH, {'items': []})
    items = data.get('items', []) if isinstance(data, dict) else []
    return [item for item in items if isinstance(item, dict)]


def save_activity_history(items, keep=300):
    clean = [item for item in (items or []) if isinstance(item, dict)]
    save_json(ACTIVITY_HISTORY_PATH, {'items': clean[-keep:]})


def activity_counts(items=None):
    items = load_activity_history() if items is None else items
    active = [item for item in items if not item.get('dismissed_at')]
    return {
        'total': len(active),
        'unread': sum(1 for item in active if not item.get('read_at')),
        'problems': sum(1 for item in active if item.get('severity') in {'warning', 'error'}),
    }


def add_activity_item(kind, title, summary, severity='info', details=None, items=None, source='trainermate', notify=True):
    now = utc_now_text()
    item = {
        'id': f"{provider_slug(kind)}_{uuid.uuid4().hex[:12]}",
        'type': kind,
        'severity': severity,
        'title': title or 'TrainerMate update',
        'summary': summary or '',
        'message': summary or '',
        'created_at': now,
        'read_at': '',
        'dismissed_at': '',
        'source': source,
        'notify': bool(notify),
        'details': details or {},
        'items': items or [],
    }
    history = load_activity_history()
    history.append(item)
    save_activity_history(history)
    return item


def parse_activity_course_key(key, course_state=None):
    text = str(key or '')
    parts = [part.strip() for part in text.split(' | ')]
    provider = parts[0] if len(parts) > 0 else ''
    course_type = parts[1] if len(parts) > 1 else ''
    date_time = parts[2] if len(parts) > 2 else ''
    return {
        'provider': provider,
        'course_type': course_type,
        'date_time': date_time,
        'status': (course_state or {}).get('status') or '',
        'action': (course_state or {}).get('last_action') or '',
        'error': (course_state or {}).get('error') or '',
    }


def build_sync_activity_from_state(state, source='manual'):
    state = state if isinstance(state, dict) else {}
    summary = state.get('run_summary') if isinstance(state.get('run_summary'), dict) else {}
    courses_state = state.get('courses') if isinstance(state.get('courses'), dict) else {}
    created = int(summary.get('db_created') or 0)
    updated = int(summary.get('db_updated') or 0)
    fobs_updated = int(summary.get('fobs_updated') or 0)
    checked = int(summary.get('courses_processed') or 0) or len(courses_state)
    issues = summary.get('health_issues') if isinstance(summary.get('health_issues'), list) else []
    failed = int(summary.get('fobs_failed') or 0) + int(summary.get('providers_failed') or 0)
    severity = 'warning' if issues or failed else 'info'
    if created or updated or fobs_updated:
        title = 'TrainerMate found updates'
        bits = []
        if created:
            bits.append(f'{created} new course' + ('' if created == 1 else 's'))
        if updated:
            bits.append(f'{updated} course update' + ('' if updated == 1 else 's'))
        if fobs_updated:
            bits.append(f'{fobs_updated} Zoom/FOBS update' + ('' if fobs_updated == 1 else 's'))
        summary_text = '. '.join(bits) + '.'
    elif severity == 'warning':
        title = 'TrainerMate needs attention'
        summary_text = str((issues or ['A provider or Zoom item needs attention.'])[0])[:220]
    else:
        title = 'TrainerMate scan complete'
        summary_text = f'{checked} course' + ('' if checked == 1 else 's') + ' checked. No action needed.'

    detail_items = []
    for key, value in sorted(courses_state.items(), key=lambda item: (item[1] or {}).get('updated_at') or '', reverse=True)[:30]:
        detail_items.append(parse_activity_course_key(key, value if isinstance(value, dict) else {}))
    details = {
        'source': source,
        'courses_checked': checked,
        'new_courses': created,
        'course_updates': updated,
        'zoom_or_fobs_updates': fobs_updated,
        'needs_attention': len(issues) + failed,
        'message': summary.get('message') or '',
        'providers': summary.get('providers') or [],
    }
    return add_activity_item('automatic_sync' if source.startswith('auto') else 'sync', title, summary_text, severity, details=details, items=detail_items, source=source)


def notification_allowed_for_activity(item):
    settings = load_automation_settings()
    if not settings.get('notifications_enabled', True) or not settings.get('popup_bubbles', True):
        return False
    if item.get('type') == 'support_message':
        return bool(settings.get('notify_support_messages', True))
    severity = item.get('severity') or 'info'
    details = item.get('details') if isinstance(item.get('details'), dict) else {}
    if severity in {'warning', 'error'}:
        return bool(settings.get('notify_problems', True))
    changed = bool((details.get('new_courses') or 0) or (details.get('course_updates') or 0) or (details.get('zoom_or_fobs_updates') or 0))
    if changed:
        return bool(settings.get('notify_course_changes', True))
    return bool(settings.get('notify_success_no_changes', False))


def latest_popup_activity():
    for item in reversed(load_activity_history()):
        if item.get('dismissed_at') or item.get('read_at'):
            continue
        if item.get('notify') is False:
            continue
        if notification_allowed_for_activity(item):
            return item
    return None


def mark_activity_read(activity_id):
    items = load_activity_history()
    now = utc_now_text()
    changed = False
    for item in items:
        if item.get('id') == activity_id and not item.get('read_at'):
            item['read_at'] = now
            changed = True
    if changed:
        save_activity_history(items)
    return changed


def dismiss_activity(activity_id):
    items = load_activity_history()
    now = utc_now_text()
    changed = False
    for item in items:
        if item.get('id') == activity_id and not item.get('dismissed_at'):
            item['dismissed_at'] = now
            if not item.get('read_at'):
                item['read_at'] = now
            changed = True
    if changed:
        save_activity_history(items)
    return changed


def compact_activity_items(limit=4):
    return [item for item in reversed(load_activity_history()) if not item.get('dismissed_at')][:limit]
