import re

from trainermate_utils import provider_slug


def normalize_ndors(value: str | None) -> str:
    return re.sub(r"\s+", "", str(value or "").strip())


def normalize_email(value: str | None) -> str:
    return str(value or "").strip().lower()


def valid_ndors_id(value: str | None) -> bool:
    text = normalize_ndors(value)
    if not text or "@" in text:
        return False
    return bool(re.match(r"^[A-Za-z0-9_-]{3,40}$", text))


def valid_email(value: str | None) -> bool:
    text = normalize_email(value)
    return bool(text and re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", text))


def mask_email(value: str | None) -> str:
    text = str(value or "").strip()
    if "@" not in text:
        return text
    name, domain = text.split("@", 1)
    if len(name) <= 2:
        masked_name = name[:1] + "-" * max(0, len(name) - 1)
    else:
        masked_name = name[:2] + "-" * max(1, len(name) - 2)
    return f"{masked_name}@{domain}"


def mask_ndors(value: str | None) -> str:
    text = normalize_ndors(value)
    if not text:
        return ""
    suffix = text[-3:] if len(text) > 3 else text[-1:]
    return ("*" * max(3, len(text) - len(suffix))) + suffix


def profile_slug_for_ndors(value: str | None) -> str:
    text = normalize_ndors(value)
    if not valid_ndors_id(text):
        return "signed-out"
    return provider_slug(text) or "signed-out"


def local_password_key(value: str | None) -> str:
    return f"password_hash::{profile_slug_for_ndors(value)}"


def remembered_login_key(value: str | None) -> str:
    return f"remembered_login::{profile_slug_for_ndors(value)}"
