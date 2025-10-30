from difflib import get_close_matches

# -------------------------------
# Clean a string (state or crop) for comparison
# -------------------------------
def clean_name(name):
    if not name:
        return ""
    return name.strip().lower()

# -------------------------------
# Return the best match from a list of options
# -------------------------------
def best_match(query, options, cutoff=0.6):
    """
    Returns the best fuzzy match from a list of options.
    :param query: string to match
    :param options: list of strings
    :param cutoff: similarity threshold (0-1)
    :return: best match or None
    """
    query = clean_name(query)
    options_clean = [clean_name(opt) for opt in options]
    matches = get_close_matches(query, options_clean, n=1, cutoff=cutoff)
    if matches:
        # Return the original option from the list
        idx = options_clean.index(matches[0])
        return options[idx], True
    return None, False
