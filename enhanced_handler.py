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
        return df, ["api"]
    except Exception as e:
        print(f"[WARN] Live API fetch failed: {e}")
        return None, []

# -------------------------------
# Helper: Load with fallback
# -------------------------------
def _load_with_fallback(resource_id, local_path):
    df, sources = fetch_live_data(resource_id)
    if df is not None:
        return df, sources
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
# Compare average rainfall
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

    sx = match_state_name(df, state_col, state_x)
    sy = match_state_name(df, state_col, state_y)

    if years:
        df = df[df[year_col].isin(years)]

    avg_x = df[df[state_col].str.lower() == sx.lower()][rain_col].mean()
    avg_y = df[df[state_col].str.lower() == sy.lower()][rain_col].mean()

    return {"answer": {"state_x_avg": float(avg_x), "state_y_avg": float(avg_y)}, "sources": sources}

# -------------------------------
# District production comparison
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
# Crop trend analysis
# -------------------------------
def analyze_crop_trend(crop, state=None, last_n_years=None):
    df_crop, sources_crop = _load_with_fallback(CROP_RESOURCE_ID, LOCAL_CROP_CSV)
    df_rain, sources_rain = _load_with_fallback(RAINFALL_RESOURCE_ID, LOCAL_RAINFALL_CSV)

    cols_crop = {c.lower(): c for c in df_crop.columns}
    cols_rain = {c.lower(): c for c in df_rain.columns}

    crop_col = cols_crop.get("crop")
    state_col_crop = cols_crop.get("state_name")
    prod_col = cols_crop.get("production_")
    year_col_crop = cols_crop.get("crop_year")

    state_col_rain = cols_rain.get("state") or cols_rain.get("state_name")
    year_col_rain = cols_rain.get("year") or cols_rain.get("crop_year")
    rain_col = cols_rain.get("avg_rainfall") or cols_rain.get("rainfall")

    df_crop[prod_col] = pd.to_numeric(df_crop[prod_col], errors="coerce")
    df_rain[rain_col] = pd.to_numeric(df_rain[rain_col], errors="coerce")

    df_crop = df_crop[df_crop[crop_col].str.lower() == crop.lower()]
    if state:
        df_crop = df_crop[df_crop[state_col_crop].str.lower() == state.lower()]
        df_rain = df_rain[df_rain[state_col_rain].str.lower() == state.lower()]

    if last_n_years:
        latest_years = sorted(df_crop[year_col_crop].unique(), reverse=True)[:last_n_years]
        df_crop = df_crop[df_crop[year_col_crop].isin(latest_years)]
        df_rain = df_rain[df_rain[year_col_rain].isin(latest_years)]

    df_merge = pd.merge(
        df_crop[[year_col_crop, prod_col, state_col_crop]],
        df_rain[[year_col_rain, rain_col, state_col_rain]],
        left_on=[year_col_crop, state_col_crop],
        right_on=[year_col_rain, state_col_rain],
        how="inner"
    )

    if df_merge.empty:
        return {"answer": {"trend": None, "rain_correlation": None}, "sources": sources_crop + sources_rain}

    correlation = df_merge[prod_col].corr(df_merge[rain_col])
    trend = df_merge.groupby(year_col_crop)[prod_col].sum().sort_index().tolist()
    years = df_merge[year_col_crop].sort_values().unique().tolist()

    return {"answer": {"years": years, "production_trend": trend, "rain_correlation": correlation}, "sources": sources_crop + sources_rain}

# -------------------------------
# Top M crops
# -------------------------------
def get_top_crops(state, top_m=3):
    df, sources = _load_with_fallback(CROP_RESOURCE_ID, LOCAL_CROP_CSV)
    cols = {c.lower(): c for c in df.columns}
    state_col = cols.get("state_name")
    crop_col = cols.get("crop")
    prod_col = cols.get("production_")

    df[state_col] = df[state_col].astype(str).str.strip().str.lower()
    df[crop_col] = df[crop_col].astype(str).str.strip().str.lower()
    df[prod_col] = pd.to_numeric(df[prod_col], errors="coerce")

    df_state = df[df[state_col] == state.lower()]
    top_crops = df_state.groupby(crop_col)[prod_col].sum().sort_values(ascending=False).head(top_m)

    return {"answer": top_crops.to_dict(), "sources": sources}

# -------------------------------
# Policy advice (area vs production)
# -------------------------------
def policy_advice(crop_a, crop_b, state):
    df, sources = _load_with_fallback(CROP_RESOURCE_ID, LOCAL_CROP_CSV)
    cols = {c.lower(): c for c in df.columns}
    prod_col = cols.get("production_")
    area_col = cols.get("area_")
    crop_col = cols.get("crop")
    state_col = cols.get("state_name")

    df[state_col] = df[state_col].astype(str).str.strip().str.lower()
    df[crop_col] = df[crop_col].astype(str).str.strip().str.lower()
    df[prod_col] = pd.to_numeric(df[prod_col], errors="coerce")
    if area_col:
        df[area_col] = pd.to_numeric(df[area_col], errors="coerce")

    df_state = df[df[state_col] == state.lower()]
    A = df_state[df_state[crop_col] == crop_a.lower()]
    B = df_state[df_state[crop_col] == crop_b.lower()]

    prodA = A[prod_col].sum() if not A.empty else 0
    prodB = B[prod_col].sum() if not B.empty else 0
    areaA = A[area_col].sum() if area_col and not A.empty else 0
    areaB = B[area_col].sum() if area_col and not B.empty else 0

    advice = []
    if prodA > prodB:
        advice.append(f"{crop_a} has higher production than {crop_b} in {state}")
    if areaA > areaB:
        advice.append(f"{crop_a} occupies larger area than {crop_b} in {state}")

    return {"answer": advice, "sources": sources}

# -------------------------------
# Main question handler
# -------------------------------
def handle_question(question):
    q = question.lower().strip()

    # 1. Rainfall comparison
    if "rainfall" in q and ("compare" in q or "average" in q):
        match = re.findall(r"(?:in\s+|between\s+)([\w\s]+?)\s*(?:and|,)\s*([\w\s]+)", q)
        years = re.findall(r"(?:for|in)\s*(\d{4})[\-–]?(\d{4})?", q)
        year_range = None
        if years:
            if years[0][1]:
                year_range = list(range(int(years[0][0]), int(years[0][1]) + 1))
            else:
                year_range = [int(years[0][0])]
        if match:
            state_x, state_y = match[0]
            return compare_states_average_rainfall(state_x.strip(), state_y.strip(), year_range)

    # 2. Crop trend
    if "trend" in q or "production trend" in q:
        match = re.findall(r"(?:trend of|production trend of)\s+([\w\s]+)(?:\s+in\s+([\w\s]+))?", q)
        years_match = re.findall(r"last\s+(\d+)", q)
        last_n_years = int(years_match[0]) if years_match else None
        if match:
            crop, state = match[0]
            state = state.strip() if state else None
            return analyze_crop_trend(crop.strip(), state, last_n_years)

    # 3. District lowest production
    if "district" in q and ("lowest production" in q or "highest production" in q):
        match = re.findall(r"district in\s+([\w\s]+)\s+has the\s+(lowest|highest)\s+production of\s+([\w\s]+)", q)
        if match:
            state, mode, crop = match[0]
            return compare_districts(crop.strip(), state.strip(), state.strip())

    # 4. Top M crops
    if "top" in q and "crop" in q:
        match = re.findall(r"top\s*(\d+)?\s*most produced crops of\s+([\w\s]+)(?:\s+in\s+([\w\s]+))?", q)
        if match:
            m, crop, state = match[0]
            m = int(m) if m else 3
            state = state.strip() if state else crop.strip()
            return get_top_crops(state, m)

    # 5. Policy advice / area vs production
    if "compare area" in q and "production" in q:
        match = re.findall(r"compare area vs production growth for\s+([\w\s]+)\s+and\s+([\w\s]+)\s+in\s+([\w\s]+)", q)
        if match:
            crop1, crop2, state = match[0]
            return policy_advice(crop1.strip(), crop2.strip(), state.strip())

    # 6. Year-on-year growth
    if "year-on-year growth" in q:
        match = re.findall(r"year-on-year growth of\s+([\w\s]+)(?:\s+in\s+([\w\s]+))?", q)
        if match:
            crop, state = match[0]
            return analyze_crop_trend(crop.strip(), state.strip() if state else None)

    return {"answer": "Sorry, I could not understand the question or missing info.", "sources": []}
