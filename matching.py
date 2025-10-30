import re
from rapidfuzz import process, fuzz

def clean_name(s: str) -> str:
    """Normalize crop/state names by removing punctuation, parentheses, and case."""
    if not s:
        return ''
    s = s.lower()
    s = re.sub(r"\(.*?\)", "", s)        # remove parentheses and contents
    s = re.sub(r"[^a-z0-9\s]", "", s)    # remove non-alphanumeric
    s = re.sub(r"\s+", " ", s).strip()   # collapse whitespace
    return s

def best_match(query: str, choices, score_cutoff: int = 70):
    """
    Return best fuzzy match from a list of choices using RapidFuzz.
    Returns a tuple: (best_match_string or None, match_score)
    """
    if not query or not choices:
        return None, 0

    q = clean_name(query)

    # Pre-clean and deduplicate choices
    cleaned_choices = {choice: clean_name(str(choice)) for choice in choices if choice}
    rev = {}
    for orig, c in cleaned_choices.items():
        if c not in rev:
            rev[c] = orig
    cleaned_list = list(rev.keys())

    # Shortcut: exact match
    if q in rev:
        return rev[q], 100

    # Perform fuzzy matching
    res = process.extractOne(q, cleaned_list, scorer=fuzz.WRatio)
    if not res:
        return None, 0

    match_clean, score, _ = res
    return (rev.get(match_clean), score) if score >= score_cutoff else (None, score)
