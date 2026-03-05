import re


def contains_any(keywords: list[str], text: str) -> bool:
    for k in keywords:
        if re.search(rf"\b{re.escape(k)}\b", text):
            return True
    return False
