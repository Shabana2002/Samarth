import os
import re
import pandas as pd
from config import CROP_RESOURCE_ID, RAINFALL_RESOURCE_ID, LOCAL_CROP_CSV, LOCAL_RAINFALL_CSV
from data_fetcher import fetch_resource_csv
from matching import clean_name, best_match

# -------------------------------
# Helper: Load data with fallback
# -------------------------------
def _load_with_fallback(resource_id, local_path, cache_name=None):
    try:
        df = fetch_resource_csv(resource_id, cache_name=cache_name)
        print(f"[INFO] Loaded live data for {resource_id} (rows={len(df)})")
        return df
    except Exception as e:
        print(f"[WARN] Live fetch failed: {e}. Falling back to local CSV: {local_path}")
        if os.path.exists(local_path):
            return pd.read_csv(local_path)
        raise FileNotFoundError(f"No live or local data available for {resource_id}")

# -------------------------------
# Rainfall comparison
# -------------------------------
def compare_states_average_rainfall(state_x, state_y, years=None, data=None):
    if data is not None:
        rain_df = pd.DataFrame(data)
    else:
        rain_df = _load_with_fallback(RAINFALL_RESOURCE_ID, LOCAL_RAINFALL_CSV, cache_name="rainfall.csv")

    cols = {c.lower(): c for c in rain_df.columns}
    state_col = cols.get("state") or cols.get("state_name")
    year_col = cols.get("year") or cols.get("crop_year")
    rain_col = cols.get("avg_rainfall") or cols.get("rainfall")

    df = rain_df[[state_col, year_col, rain_col]].copy()
    df[state_col] = df[state_col].astype(str).str.strip().str.lower()
    df[year_col] = pd.to_numeric(df[year_col], errors="coerce").fillna(0).astype(int)
    df[rain_col] = pd.to_numeric(df[rain_col], errors="coerce")

    if years:
        df = df[df[year_col].isin(years)]

    avg_x = df[df[state_col] == state_x.lower()][rain_col].mean()
    avg_y = df[df[state_col] == state_y.lower()][rain_col].mean()
    return {"state_x_avg": float(avg_x), "state_y_avg": float(avg_y)}

# -------------------------------
# District-level crop comparison
# -------------------------------
def compare_districts(crop, state1, state2, data=None):
    if data is not None:
        crop_df = pd.DataFrame(data)
    else:
        crop_df = _load_with_fallback(CROP_RESOURCE_ID, LOCAL_CROP_CSV, cache_name="crop.csv")

    cols = {c.lower(): c for c in crop_df.columns}
    state_col = cols.get("state_name")
    district_col = cols.get("district_name")
    crop_col = cols.get("crop")
    prod_col = cols.get("production_")

    crop_df[state_col] = crop_df[state_col].astype(str).str.strip().str.lower()
    crop_df[crop_col] = crop_df[crop_col].astype(str).str.strip().str.lower()
    crop_df[district_col] = crop_df[district_col].astype(str).str.strip().str.lower()
    crop_df[prod_col] = pd.to_numeric(crop_df[prod_col], errors="coerce")

    crop_df["__crop_clean"] = crop_df[crop_col].map(clean_name)
    desired_clean = clean_name(crop)
    unique_crops = crop_df[crop_col].dropna().unique().tolist()
    match, _ = best_match(crop.lower(), unique_crops)

    filtered = crop_df[crop_df["__crop_clean"] == desired_clean] if match is None else crop_df[crop_df[crop_col] == match]
    df1 = filtered[filtered[state_col] == state1.lower()]
    df2 = filtered[filtered[state_col] == state2.lower()]

    if df1.empty or df2.empty:
        return {"state1_top": None, "state2_low": None}

    grouped1 = df1.groupby(district_col)[prod_col].sum()
    grouped2 = df2.groupby(district_col)[prod_col].sum()

    high1 = grouped1.idxmax()
    high1_val = grouped1.max()
    low2 = grouped2.idxmin()
    low2_val = grouped2.min()

    return {"state1_top": (high1, float(high1_val)), "state2_low": (low2, float(low2_val))}

# -------------------------------
# Policy advice
# -------------------------------
def year_on_year_growth(series):
    pct_change = series.pct_change().dropna()
    return pct_change.mean() if not pct_change.empty else None

def policy_advice(crop_a, crop_b, state, years=None, top_n=3, data=None):
    if data is not None:
        crop_df = pd.DataFrame(data)
    else:
        crop_df = _load_with_fallback(CROP_RESOURCE_ID, LOCAL_CROP_CSV, cache_name="crop.csv")

    cols = {c.lower(): c for c in crop_df.columns}
    prod_col = cols.get("production_")
    area_col = cols.get("area_")
    crop_col = cols.get("crop")
    state_col = cols.get("state_name")
    year_col = cols.get("crop_year")

    crop_df[state_col] = crop_df[state_col].astype(str).str.strip().str.lower()
    crop_df[crop_col] = crop_df[crop_col].astype(str).str.strip().str.lower()
    crop_df[prod_col] = pd.to_numeric(crop_df[prod_col], errors="coerce")
    if area_col:
        crop_df[area_col] = pd.to_numeric(crop_df[area_col], errors="coerce")

    df = crop_df.copy()
    if years and year_col:
        df = df[df[year_col].isin(years)]
    df = df[df[state_col] == state.lower()]

    A = df[df[crop_col] == crop_a.lower()]
    B = df[df[crop_col] == crop_b.lower()]

    prodA = A[prod_col].sum() if not A.empty else 0
    prodB = B[prod_col].sum() if not B.empty else 0
    areaA = A[area_col].sum() if area_col and not A.empty else 0
    areaB = B[area_col].sum() if area_col and not B.empty else 0

    trends = {}
    if year_col:
        trends["A_growth"] = year_on_year_growth(A.groupby(year_col)[prod_col].sum())
        trends["B_growth"] = year_on_year_growth(B.groupby(year_col)[prod_col].sum())

    args = []
    if prodA > prodB:
        args.append(f"{crop_a} has higher total production ({prodA:.0f}) than {crop_b} ({prodB:.0f}) in {state}.")
    elif prodB > prodA:
        args.append(f"{crop_b} has higher total production ({prodB:.0f}) than {crop_a} ({prodA:.0f}) in {state}.")

    if areaA and areaB:
        if areaA > areaB:
            args.append(f"{crop_a} occupies more cultivated area ({areaA:.0f}) than {crop_b} ({areaB:.0f}).")
        elif areaB > areaA:
            args.append(f"{crop_b} occupies more cultivated area ({areaB:.0f}) than {crop_a} ({areaA:.0f}).")

    if trends.get("A_growth") is not None and trends.get("B_growth") is not None:
        ga, gb = trends["A_growth"], trends["B_growth"]
        if ga > gb:
            args.append(f"{crop_a} shows stronger average year-on-year growth ({ga:.2%}) than {crop_b} ({gb:.2%}).")
        else:
            args.append(f"{crop_b} shows stronger average year-on-year growth ({gb:.2%}) than {crop_a} ({ga:.2%}).")

    return args[:top_n]

# -------------------------------
# Question handler
# -------------------------------
STATE_LIST = [
    "Andhra Pradesh", "Karnataka", "Maharashtra", "Punjab", "Haryana",
    "Rajasthan", "West Bengal", "Uttar Pradesh", "Tamil Nadu", "Gujarat", "Madhya Pradesh"
]

def handle_question(question):
    question_lower = question.lower()
    intent = None

    # Intent detection
    if "rainfall" in question_lower:
        intent = "rainfall_comparison"
    elif "production" in question_lower or "district" in question_lower:
        intent = "crop_comparison"
    elif "policy" in question_lower or "better to cultivate" in question_lower:
        intent = "policy_advice"
    elif "growth" in question_lower:
        intent = "multi_year_trend"

    # Extract states and years
    states = [s for s in STATE_LIST if s.lower() in question_lower]
    years = [int(y) for y in re.findall(r"\b(20\d{2})\b", question)]

    # Extract crops (simplified: words between "of" or "for" and "in")
    crop1, crop2 = None, None
    crop_match = re.findall(r"(?:of|for)\s+([\w\s]+?)(?:\s+and\s+([\w\s]+))?\s+in", question_lower)
    if crop_match:
        crop1 = crop_match[0][0].strip()
        crop2 = crop_match[0][1].strip() if crop_match[0][1] else None

    # Dispatch
    if intent == "rainfall_comparison" and len(states) >= 2:
        return compare_states_average_rainfall(states[0], states[1], years)
    elif intent == "crop_comparison" and len(states) >= 1 and crop1:
        return compare_districts(crop1, states[0], states[1] if len(states) > 1 else states[0])
    elif intent == "policy_advice" and len(states) >= 1 and crop1 and crop2:
        return policy_advice(crop1, crop2, states[0], years)
    else:
        return {"answer": f"Could not map your question to a supported pattern. Detected intent={intent}, params={{'crop': '{crop1}', 'states': {states}, 'years': {years}}}"}

