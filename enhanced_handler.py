import os
import re
import pandas as pd
import requests
from config import (
    DATA_GOV_API_KEY,
    CROP_RESOURCE_ID,
    RAINFALL_RESOURCE_ID,
    LOCAL_CROP_CSV,
    LOCAL_RAINFALL_CSV
)
from matching import clean_name, best_match

# -------------------------------
# Helper: Fetch live data from data.gov.in
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
# Helper: Load with fallback
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
# Utility: Fuzzy match state name
# -------------------------------
def match_state_name(df, col, query):
    query = query.lower().strip()
    matches = df[col].astype(str).str.lower().str.strip().unique().tolist()
    best, score = best_match(query, matches)
    return best if best else query

# -------------------------------
# Compare rainfall averages
# -------------------------------
def compare_states_average_rainfall(state_x, state_y, years=None):
    df, sources = _load_with_fallback(RAINFALL_RESOURCE_ID, LOCAL_RAINFALL_CSV)
    cols = {c.lower(): c for c in df.columns}
    state_col = cols.get("state") or cols.get("state_name")
    year_col = cols.get("year") or cols.get("crop_year")
    rain_col = cols.get("avg_rainfall") or cols.get("rainfall") or cols.get("annual_rainfall")

    df[state_col] = df[state_col].astype(str).str.strip()
    df[year_col] = pd.to_numeric(df[year_col], errors="coerce")
    df[rain_col] = pd.to_numeric(df[rain_col], errors="coerce")

    # fuzzy match
    sx = match_state_name(df, state_col, state_x)
    sy = match_state_name(df, state_col, state_y)

    if years:
        df = df[df[year_col].isin(years)]

    avg_x = df[df[state_col].str.lower() == sx.lower()][rain_col].mean()
    avg_y = df[df[state_col].str.lower() == sy.lower()][rain_col].mean()

    return {"answer": {"state_x_avg": float(avg_x), "state_y_avg": float(avg_y)}, "sources": sources}

# -------------------------------
# NEW: Top M crops per state
# -------------------------------
def get_top_crops(state_x, state_y, crop_type=None, years=None, top_m=3):
    df, sources = _load_with_fallback(CROP_RESOURCE_ID, LOCAL_CROP_CSV)
    cols = {c.lower(): c for c in df.columns}
    state_col = cols.get("state_name")
    crop_col = cols.get("crop")
    prod_col = cols.get("production_")
    year_col = cols.get("crop_year")

    df[state_col] = df[state_col].astype(str).str.strip().str.lower()
    df[crop_col] = df[crop_col].astype(str).str.strip().str.lower()
    df[prod_col] = pd.to_numeric(df[prod_col], errors="coerce")

    if years:
        df = df[df[year_col].isin(years)]

    results = {}
    for st in [state_x, state_y]:
        st_clean = st.lower()
        sub = df[df[state_col] == st_clean]
        if crop_type:
            sub = sub[sub[crop_col].str.contains(crop_type.lower(), na=False)]
        top = sub.groupby(crop_col)[prod_col].sum().nlargest(top_m)
        results[st] = top.to_dict()

    return {"answer": results, "sources": sources}

# -------------------------------
# Combine Rainfall + Top Crops
# -------------------------------
def compare_rainfall_and_top_crops(state_x, state_y, crop_type=None, years=None, top_m=3):
    rain = compare_states_average_rainfall(state_x, state_y, years)
    crops = get_top_crops(state_x, state_y, crop_type, years, top_m)
    return {
        "answer": {
            "rainfall": rain["answer"],
            "top_crops": crops["answer"]
        },
        "sources": rain["sources"] + crops["sources"]
    }

# -------------------------------
# Compare highest & lowest district crop production
# -------------------------------
def compare_districts(crop, state1, state2):
    df, sources = _load_with_fallback(CROP_RESOURCE_ID, LOCAL_CROP_CSV)
    cols = {c.lower(): c for c in df.columns}
    state_col = cols.get("state_name")
    district_col = cols.get("district_name")
    crop_col = cols.get("crop")
    prod_col = cols.get("production_")

    df[state_col] = df[state_col].astype(str).str.strip().str.lower()
    df[crop_col] = df[crop_col].astype(str).str.strip().str.lower()
    df[district_col] = df[district_col].astype(str).str.strip().str.lower()
    df[prod_col] = pd.to_numeric(df[prod_col], errors="coerce")

    df1 = df[(df[state_col] == state1.lower()) & (df[crop_col] == crop.lower())]
    df2 = df[(df[state_col] == state2.lower()) & (df[crop_col] == crop.lower())]

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
# NEW: Crop trend + climate correlation
# -------------------------------
def analyze_crop_trend(crop, region, years=None):
    crop_df, csrc = _load_with_fallback(CROP_RESOURCE_ID, LOCAL_CROP_CSV)
    rain_df, rsrc = _load_with_fallback(RAINFALL_RESOURCE_ID, LOCAL_RAINFALL_CSV)

    # normalize
    crop_df.columns = [c.lower() for c in crop_df.columns]
    rain_df.columns = [c.lower() for c in rain_df.columns]

    crop_df = crop_df.rename(columns={"state_name": "state", "crop_year": "year"})
    rain_df = rain_df.rename(columns={"state_name": "state"})
    crop_df["state"] = crop_df["state"].str.lower().str.strip()
    rain_df["state"] = rain_df["state"].str.lower().str.strip()
    crop_df["production_"] = pd.to_numeric(crop_df["production_"], errors="coerce")
    rain_df["rainfall"] = pd.to_numeric(rain_df.get("rainfall") or rain_df.get("avg_rainfall"), errors="coerce")

    # filter
    region_clean = region.lower()
    crop_clean = crop.lower()
    csub = crop_df[(crop_df["state"] == region_clean) & (crop_df["crop"] == crop_clean)]
    rsub = rain_df[rain_df["state"] == region_clean]

    # merge & aggregate
    merged = pd.merge(csub, rsub, on=["state", "year"], how="inner")
    trend = csub.groupby("year")["production_"].sum().reset_index()

    if len(trend) < 2:
        return {"answer": "Insufficient data for trend analysis.", "sources": csrc + rsrc}

    correlation = merged["production_"].corr(merged["rainfall"])
    direction = "increasing" if trend["production_"].iloc[-1] > trend["production_"].iloc[0] else "decreasing"

    return {
        "answer": {
            "trend_direction": direction,
            "correlation_with_rainfall": float(correlation) if correlation else None,
            "years_covered": int(trend["year"].nunique())
        },
        "sources": csrc + rsrc
    }

# -------------------------------
# Handle incoming questions
# -------------------------------
STATE_LIST = [
    "Andhra Pradesh", "Arunachal Pradesh", "Assam", "Bihar", "Chhattisgarh",
    "Goa", "Gujarat", "Haryana", "Himachal Pradesh", "Jharkhand",
    "Karnataka", "Kerala", "Madhya Pradesh", "Maharashtra", "Manipur",
    "Meghalaya", "Mizoram", "Nagaland", "Odisha", "Punjab",
    "Rajasthan", "Sikkim", "Tamil Nadu", "Telangana", "Tripura",
    "Uttar Pradesh", "Uttarakhand", "West Bengal",
    "Andaman and Nicobar Islands", "Chandigarh", "Delhi",
    "Jammu and Kashmir", "Ladakh", "Puducherry"
]


def handle_question(question):
    question_lower = question.lower()
    intent = None

    if "rainfall" in question_lower and "crop" in question_lower:
        intent = "rainfall_and_crops"
    elif "rainfall" in question_lower:
        intent = "rainfall_comparison"
    elif "production" in question_lower and "district" in question_lower:
        intent = "crop_comparison"
    elif "policy" in question_lower or "better to cultivate" in question_lower:
        intent = "policy_advice"
    elif "trend" in question_lower or "analyze" in question_lower:
        intent = "trend_analysis"

    # extract entities
    states = [s for s in STATE_LIST if s.lower() in question_lower]
    years = [int(y) for y in re.findall(r"\b(20\d{2})\b", question)]
    crop_match = re.findall(r"(?:of|for)\s+([\w\s]+?)(?:\s+and\s+([\w\s]+))?(?:\?|$)", question_lower)
    crop1 = crop_match[0][0].strip() if crop_match else None
    crop2 = crop_match[0][1].strip() if crop_match and crop_match[0][1] else None

    # route logic
    if intent == "rainfall_and_crops" and len(states) >= 2:
        return compare_rainfall_and_top_crops(states[0], states[1], crop1, years)
    elif intent == "rainfall_comparison" and len(states) >= 2:
        return compare_states_average_rainfall(states[0], states[1], years)
    elif intent == "crop_comparison" and len(states) >= 2 and crop1:
        return compare_districts(crop1, states[0], states[1])
    elif intent == "policy_advice" and len(states) >= 1 and crop1 and crop2:
        return policy_advice(crop1, crop2, states[0], years)
    elif intent == "trend_analysis" and len(states) >= 1 and crop1:
        return analyze_crop_trend(crop1, states[0], years)
    else:
        return {"answer": "Sorry, I could not understand the question or missing info.", "sources": []}
