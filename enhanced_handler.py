import os
import re
import pandas as pd
import requests
from config import DATA_GOV_API_KEY, CROP_RESOURCE_ID, RAINFALL_RESOURCE_ID, LOCAL_CROP_CSV, LOCAL_RAINFALL_CSV
from matching import clean_name, best_match

# -------------------------------
# Helper: Fetch live CSV from API
# -------------------------------
def fetch_live_data(resource_id, limit=5000):
    url = f"https://api.data.gov.in/resource/{resource_id}?api-key={DATA_GOV_API_KEY}&format=json&limit={limit}"
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json().get("records", [])
        if not data:
            raise ValueError("No records returned from API.")
        df = pd.DataFrame(data)
        return df, "api"
    except Exception as e:
        print(f"[WARN] Live API fetch failed: {e}")
        return None, None

# -------------------------------
# Helper: Load data with fallback
# -------------------------------
def _load_with_fallback(resource_id, local_path):
    df, source = fetch_live_data(resource_id)
    if df is not None:
        return df, [source]
    elif os.path.exists(local_path):
        print(f"[INFO] Using local CSV fallback: {local_path}")
        df = pd.read_csv(local_path)
        return df, [local_path]
    else:
        raise FileNotFoundError(f"No live or local data available for {resource_id}")

# -------------------------------
# Rainfall comparison
# -------------------------------
def compare_states_average_rainfall(state_x, state_y, years=None):
    df, sources = _load_with_fallback(RAINFALL_RESOURCE_ID, LOCAL_RAINFALL_CSV)

    # Normalize column names
    cols = {c.lower(): c for c in df.columns}
    state_col = cols.get("state") or cols.get("state_name")
    year_col = cols.get("year") or cols.get("crop_year")
    rain_col = cols.get("avg_rainfall") or cols.get("rainfall")

    df[state_col] = df[state_col].astype(str).str.strip().str.lower()
    df[year_col] = pd.to_numeric(df[year_col], errors="coerce").fillna(0).astype(int)
    df[rain_col] = pd.to_numeric(df[rain_col], errors="coerce")

    if years:
        df = df[df[year_col].isin(years)]

    avg_x = df[df[state_col] == state_x.lower()][rain_col].mean()
    avg_y = df[df[state_col] == state_y.lower()][rain_col].mean()

    return {"answer": {"state_x_avg": float(avg_x), "state_y_avg": float(avg_y)}, "sources": sources}

# -------------------------------
# Crop comparison
# -------------------------------
def compare_districts(crop, state1, state2):
    df, sources = _load_with_fallback(CROP_RESOURCE_ID, LOCAL_CROP_CSV)

    # Normalize column names
    cols = {c.lower(): c for c in df.columns}
    state_col = cols.get("state_name")
    district_col = cols.get("district_name")
    crop_col = cols.get("crop")
    prod_col = cols.get("production_")

    df[state_col] = df[state_col].astype(str).str.strip().str.lower()
    df[crop_col] = df[crop_col].astype(str).str.strip().str.lower()
    df[district_col] = df[district_col].astype(str).str.strip().str.lower()
    df[prod_col] = pd.to_numeric(df[prod_col], errors="coerce")

    df["__crop_clean"] = df[crop_col].map(clean_name)
    desired_clean = clean_name(crop)
    unique_crops = df[crop_col].dropna().unique().tolist()
    match, _ = best_match(crop.lower(), unique_crops)

    filtered = df[df["__crop_clean"] == desired_clean] if match is None else df[df[crop_col] == match]
    df1 = filtered[filtered[state_col] == state1.lower()]
    df2 = filtered[filtered[state_col] == state2.lower()]

    if df1.empty or df2.empty:
        return {"answer": {"state1_top": None, "state2_low": None}, "sources": sources}

    grouped1 = df1.groupby(district_col)[prod_col].sum()
    grouped2 = df2.groupby(district_col)[prod_col].sum()

    high1 = grouped1.idxmax()
    high1_val = grouped1.max()
    low2 = grouped2.idxmin()
    low2_val = grouped2.min()

    return {"answer": {"state1_top": (high1, float(high1_val)), "state2_low": (low2, float(low2_val))}, "sources": sources}

# -------------------------------
# Policy advice
# -------------------------------
def year_on_year_growth(series):
    pct_change = series.pct_change().dropna()
    return pct_change.mean() if not pct_change.empty else None

def policy_advice(crop_a, crop_b, state, years=None, top_n=3):
    df, sources = _load_with_fallback(CROP_RESOURCE_ID, LOCAL_CROP_CSV)

    cols = {c.lower(): c for c in df.columns}
    prod_col = cols.get("production_")
    area_col = cols.get("area_")
    crop_col = cols.get("crop")
    state_col = cols.get("state_name")
    year_col = cols.get("crop_year")

    df[state_col] = df[state_col].astype(str).str.strip().str.lower()
    df[crop_col] = df[crop_col].astype(str).str.strip().str.lower()
    df[prod_col] = pd.to_numeric(df[prod_col], errors="coerce")
    if area_col:
        df[area_col] = pd.to_numeric(df[area_col], errors="coerce")

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

    return {"answer": args[:top_n], "sources": sources}

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

    if "rainfall" in question_lower:
        intent = "rainfall_comparison"
    elif "production" in question_lower or "district" in question_lower:
        intent = "crop_comparison"
    elif "policy" in question_lower or "better to cultivate" in question_lower:
        intent = "policy_advice"

    # Extract states and years
    states = [s for s in STATE_LIST if s.lower() in question_lower]
    years = [int(y) for y in re.findall(r"\b(20\d{2})\b", question)]

    # Extract crops (simplified)
    crop1, crop2 = None, None
    crop_match = re.findall(r"(?:of|for)\s+([\w\s]+?)(?:\s+and\s+([\w\s]+))?(?:\?|$)", question_lower)
    if crop_match:
        crop1 = crop_match[0][0].strip() if crop_match[0][0] else None
        crop2 = crop_match[0][1].strip() if crop_match[0][1] else None

    if intent == "rainfall_comparison" and len(states) >= 2:
        return compare_states_average_rainfall(states[0], states[1], years)
    elif intent == "crop_comparison" and len(states) >= 2 and crop1:
        return compare_districts(crop1, states[0], states[1])
    elif intent == "policy_advice" and len(states) >= 1 and crop1 and crop2:
        return policy_advice(crop1, crop2, states[0], years)
    else:
        return {"answer": "Sorry, I could not understand the question or missing info.", "sources": []}

