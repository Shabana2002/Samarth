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

    sx = match_state_name(df, state_col, state_x)
    sy = match_state_name(df, state_col, state_y)

    if years:
        df = df[df[year_col].isin(years)]

    avg_x = df[df[state_col].str.lower() == sx.lower()][rain_col].mean()
    avg_y = df[df[state_col].str.lower() == sy.lower()][rain_col].mean()

    return {"answer": {"state_x_avg": float(avg_x), "state_y_avg": float(avg_y)}, "sources": sources}

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

    if areaA > areaB:
        args.append(f"{crop_a} occupies more area ({areaA:.0f}) than {crop_b} ({areaB:.0f}) in {state}.")
    elif areaB > areaA:
        args.append(f"{crop_b} occupies more area ({areaB:.0f}) than {crop_a} ({areaA:.0f}) in {state}.")

    if trends.get("A_growth") and trends.get("B_growth"):
        if trends["A_growth"] > trends["B_growth"]:
            args.append(f"{crop_a} shows stronger year-on-year growth than {crop_b}.")
        else:
            args.append(f"{crop_b} shows stronger year-on-year growth than {crop_a}.")

    return {"answer": {"advice": args}, "sources": sources}

# -------------------------------
# Crop trend analysis with rainfall
# -------------------------------
def analyze_crop_trend(crop, state=None):
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

    return {
        "answer": {"years": years, "production_trend": trend, "rain_correlation": correlation},
        "sources": sources_crop + sources_rain
    }

# -------------------------------
# Get top crops for a state
# -------------------------------
def get_top_crops(state, top_m=5):
    df, sources = _load_with_fallback(CROP_RESOURCE_ID, LOCAL_CROP_CSV)
    cols = {c.lower(): c for c in df.columns}
    state_col = cols.get("state_name")
    crop_col = cols.get("crop")
    prod_col = cols.get("production_")

    df[state_col] = df[state_col].astype(str).str.strip().str.lower()
    df[crop_col] = df[crop_col].astype(str).str.strip().str.lower()
    df[prod_col] = pd.to_numeric(df[prod_col], errors="coerce")

    df_state = df[df[state_col] == state.lower()]
    if df_state.empty:
        return {"answer": [], "sources": sources}

    grouped = df_state.groupby(crop_col)[prod_col].sum()
    top_crops = grouped.sort_values(ascending=False).head(top_m)

    return {"answer": top_crops.reset_index().values.tolist(), "sources": sources}

# -------------------------------
# Main NLP handler
# -------------------------------
def handle_question(question: str):
    question = question.lower()

    if "rainfall" in question and "average" in question and "between" in question:
        match = re.findall(r"between\s+([\w\s]+?)\s+and\s+([\w\s]+)", question)
        if match:
            state_x, state_y = match[0]
            return compare_states_average_rainfall(state_x, state_y)

    if "highest" in question and "lowest" in question and "production" in question:
        match = re.findall(r"for\s+([\w\s]+)\s+in\s+([\w\s]+)\s+and\s+([\w\s]+)", question)
        if match:
            crop, state1, state2 = match[0]
            return compare_districts(crop, state1, state2)

    if "policy" in question or "advise" in question:
        match = re.findall(r"(?:between|for)\s+([\w\s]+)\s+and\s+([\w\s]+)\s+in\s+([\w\s]+)", question)
        if match:
            crop_a, crop_b, state = match[0]
            return policy_advice(crop_a, crop_b, state)

    if "trend" in question or "correlation" in question:
        match = re.findall(r"trend\s+of\s+([\w\s]+)(?:\s+in\s+([\w\s]+))?", question)
        if match:
            crop, state = match[0]
            state = state.strip() if state else None
            return analyze_crop_trend(crop, state)

    return {"answer": None, "sources": []}


from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route('/ask', methods=['POST'])
def ask():
    data = request.json
    question = data.get('question', '')
    result = handle_question(question)
    return jsonify(result)

if __name__ == '__main__':
    app.run(debug=True)
