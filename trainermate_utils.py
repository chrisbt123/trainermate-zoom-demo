def provider_slug(value):
    cleaned = ''.join(ch.lower() if ch.isalnum() else '-' for ch in (value or '').strip())
    while '--' in cleaned:
        cleaned = cleaned.replace('--', '-')
    return cleaned.strip('-') or 'provider'
