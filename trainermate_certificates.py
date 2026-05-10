from trainermate_utils import provider_slug


def valid_document_provider_ids(provider_ids, providers):
    provider_set = {provider_slug(p.get('id') or p.get('name') or '') for p in providers or []}
    valid = []
    seen = set()
    for raw_provider_id in provider_ids or []:
        provider_id = provider_slug(raw_provider_id)
        if not provider_id or provider_id in seen or provider_id not in provider_set:
            continue
        valid.append(provider_id)
        seen.add(provider_id)
    return valid


def all_document_provider_ids(providers):
    return valid_document_provider_ids([p.get('id') or p.get('name') for p in providers or []], providers)


def selected_document_provider_ids(form, providers):
    if (form.get('use_all_providers') or '').strip() == '1':
        return all_document_provider_ids(providers)
    return valid_document_provider_ids(form.getlist('provider_ids'), providers)
