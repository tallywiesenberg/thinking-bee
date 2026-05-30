import re
from html import unescape


def remove_wiki_usernames(text):
    return re.sub(r"\b[a-zA-Z]*editor\d+\b", " ", text, flags=re.IGNORECASE)


def looks_like_username(token):
    return len(token) > 10 or any(char.isdigit() for char in token)


def contains_keywords(text, keywords):
    if not isinstance(text, str):
        return 0
    pattern = "|".join(map(re.escape, keywords))
    return int(bool(re.search(pattern, text, flags=re.IGNORECASE)))

def clean_html(text):
    if not isinstance(text, str):
        return ""

    # Unescape HTML entities (&amp;, &lt;, etc.)
    text = unescape(text)

    # Remove HTML tags
    text = re.sub(r"<[^>]+>", " ", text)

    # Remove URLs
    text = re.sub(r"http\S+|www\S+", " ", text)

    # Remove leftover markup artifacts
    text = re.sub(r"\{\{.*?\}\}", " ", text)  # templates
    text = re.sub(r"\[\[|\]\]", " ", text)  # wiki links
    text = re.sub(r"\|", " ", text)

    # Keep only words (remove weird punctuation)
    text = re.sub(r"[^a-zA-Z\s]", " ", text)

    # Normalize whitespace
    text = re.sub(r"\s+", " ", text).strip()

    return text.lower()


def extract_added_text(html):
    if not isinstance(html, str):
        return ""

    added = re.findall(r"<ins[^>]*>(.*?)</ins>", html, flags=re.DOTALL)
    added_text = " ".join(added)

    return clean_html(added_text)
