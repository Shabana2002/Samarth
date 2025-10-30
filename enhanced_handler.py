import pandas as pd
import requests
from config import DATA_GOV_API_KEY, CROP_RESOURCE_ID, RAINFALL_RESOURCE_ID

# ------------------------
# FETCH AND PREPROCESS DATA
# ------------------------
def fetch_data(resource_id=None, resource_type='crop', csv_file=None):
    """
    Fetch data from API (live) or CSV fallback.
    """
    df = pd.DataFrame()
    if resource_id:
        try:
            url = f"https://api.data.gov.in/resource/{resource_id}?api-key={DATA_GOV_API_KEY}&format=json&offset=0&limit=50000"
            r = requests.get(url, timeout=10)
            r.raise_for_status()
            data = r.json()
            records = data.get('records', [])
            df = pd.DataFrame(records)
            print(f"INFO: Fetched {len(df)} rows from API for {resource_type}")
        except Exception as e:
            print(f"WARNING: API fetch failed for {resource_type}: {e}")

    if df.empty and csv_file:
        df = pd.read_csv(csv_file)
        print(f"INFO: Loaded {len(df)} rows from CSV for {resource_type}")

    if df.empty:
        print(f"ERROR: No data found for {resource_type}")
        return pd.DataFrame()

    # ------------------------
    # Normalize columns
    # ------------------------
    df.columns = [c.strip().lower() for c in df.columns]

    if resource_type == 'crop':
        rename_map = {
            'state_name': 'State',
            'district_name': 'District',
            'crop_year': 'Year',
            'season': 'Season',
            'crop': 'Crop',
            'area_': 'Area',
            'production_': 'Production'
        }
    else:  # rainfall
        rename_map = {
            'state': 'State',
            'district': 'District',
            'year': 'Year',
            'month': 'Month',  # optional
            'rainfall': 'Rainfall'
        }

    df = df.rename(columns=rename_map)

    # Normalize strings
    for col in ['State', 'District', 'Crop']:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.lower()

    # Convert numeric columns
    for col in ['Year', 'Rainfall', 'Production', 'Area']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    return df

# ------------------------
# LOAD DATA
# ------------------------
crop_df = fetch_data(resource_type='crop', resource_id=CROP_RESOURCE_ID, csv_file='crop_data.csv')
rainfall_df = fetch_data(resource_type='rainfall', resource_id=RAINFALL_RESOURCE_ID, csv_file='rainfall_data.csv')

# ------------------------
# ANALYSIS FUNCTIONS
# ------------------------
def compare_avg_rainfall(states, last_n_years=None):
    df = rainfall_df[rainfall_df['State'].isin([s.lower() for s in states])]
    if last_n_years:
        recent_years = sorted(df['Year'].dropna().unique(), reverse=True)[:last_n_years]
        df = df[df['Year'].isin(recent_years)]
    result = df.groupby('State')['Rainfall'].mean().reset_index()
    result['source'] = f"https://data.gov.in/resource/{RAINFALL_RESOURCE_ID}"
    return result.to_dict(orient='records')

def top_m_crops(states, crop_type, m=5, last_n_years=None, specific_year=None):
    df = crop_df[crop_df['State'].isin([s.lower() for s in states])]
    df = df[df['Crop'].str.contains(crop_type.lower(), case=False)]
    if specific_year:
        df = df[df['Year'] == specific_year]
    elif last_n_years:
        recent_years = sorted(df['Year'].dropna().unique(), reverse=True)[:last_n_years]
        df = df[df['Year'].isin(recent_years)]
    results = []
    for state in states:
        top_c = df[df['State'] == state.lower()].groupby('Crop')['Production'].sum().reset_index().sort_values('Production', ascending=False).head(m)
        results.append({
            'State': state,
            'TopCrops': top_c.to_dict(orient='records'),
            'source': f"https://data.gov.in/resource/{CROP_RESOURCE_ID}"
        })
    return results

def district_high_low_crop(states, crop, specific_year=None):
    df = crop_df[crop_df['State'].isin([s.lower() for s in states])]
    df = df[df['Crop'].str.contains(crop.lower(), case=False)]
    if specific_year:
        df = df[df['Year'] == specific_year]
    results = []
    for state in states:
        df_state = df[df['State'] == state.lower()]
        if df_state.empty:
            continue
        high = df_state.loc[df_state['Production'].idxmax()]
        low = df_state.loc[df_state['Production'].idxmin()]
        results.append({
            'State': state,
            'Highest': {'District': high['District'], 'Production': high['Production']},
            'Lowest': {'District': low['District'], 'Production': low['Production']},
            'source': f"https://data.gov.in/resource/{CROP_RESOURCE_ID}"
        })
    return results

def production_trend(state, crop, years=10, specific_year=None):
    df = crop_df[(crop_df['State'] == state.lower()) & (crop_df['Crop'].str.contains(crop.lower(), case=False))]
    if specific_year:
        df = df[df['Year'] == specific_year]
    else:
        recent_years = sorted(df['Year'].dropna().unique(), reverse=True)[:years]
        df = df[df['Year'].isin(recent_years)]
    trend = df.groupby('Year')['Production'].sum().reset_index()
    return {
        'State': state,
        'Crop': crop,
        'Trend': trend.to_dict(orient='records'),
        'source': f"https://data.gov.in/resource/{CROP_RESOURCE_ID}"
    }

def rainfall_correlation(state, crop, years=10):
    df_crop = crop_df[(crop_df['State'] == state.lower()) & (crop_df['Crop'].str.contains(crop.lower(), case=False))]
    df_rain = rainfall_df[rainfall_df['State'] == state.lower()]
    common_years = sorted(set(df_crop['Year'].dropna().unique()) & set(df_rain['Year'].dropna().unique()), reverse=True)[:years]
    df_crop = df_crop[df_crop['Year'].isin(common_years)]
    df_rain = df_rain[df_rain['Year'].isin(common_years)]
    merged = pd.merge(df_crop.groupby('Year')['Production'].sum().reset_index(),
                      df_rain[['Year','Rainfall']], on='Year')
    corr = merged['Production'].corr(merged['Rainfall']) if not merged.empty else None
    return {
        'State': state,
        'Crop': crop,
        'Correlation': corr,
        'source_crop': f"https://data.gov.in/resource/{CROP_RESOURCE_ID}",
        'source_rainfall': f"https://data.gov.in/resource/{RAINFALL_RESOURCE_ID}"
    }
