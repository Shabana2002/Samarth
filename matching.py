import re
from rapidfuzz import process, fuzz

def clean_name(s: str) -> str:
    if s is None:
        return ''
    s = s.lower()
    # remove parentheses and content inside them, punctuation, and extra whitespace
    s = re.sub(r"\(.*?\)", "", s)
    s = re.sub(r"[^a-z0-9\s]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def best_match(query: str, choices, score_cutoff=70):
    """Return best match from choices (list or iterable of strings).
       Uses rapidfuzz for fuzzy matching. Returns (match, score).
    """
    q = clean_name(query)
    # Pre-clean choices into tuple of (original, cleaned)
    cleaned_choices = {choice: clean_name(choice) for choice in choices}
    # rapidfuzz's process.extractOne expects iterable of strings (we'll use cleaned values map back)
    # Build mapping from cleaned -> original (if collisions happen, prefer first)
    rev = {}
    for orig, c in cleaned_choices.items():
        if c not in rev:
            rev[c] = orig
    cleaned_list = list(rev.keys())
    res = process.extractOne(q, cleaned_list, scorer=fuzz.WRatio)
    if res is None:
        return None, 0
    match_clean, score, _ = res
    return rev.get(match_clean), score if score >= score_cutoff else (None, score)
