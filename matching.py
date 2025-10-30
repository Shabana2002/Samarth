import re
from enhanced_handler import (
    top_m_crops,
    compare_avg_rainfall,
    production_trend,
    district_high_low_crop,
    rainfall_correlation
)

# ------------------------
# Helper functions
# ------------------------

VALID_STATES = [
    'andaman and nicobar islands', 'arunachal pradesh', 'assam', 'bihar',
    'chhattisgarh', 'goa', 'gujarat', 'haryana', 'himachal pradesh',
    'jammu and kashmir', 'jharkhand', 'karnataka', 'kerala', 'madhya pradesh',
    'maharashtra', 'manipur', 'meghalaya', 'mizoram', 'nagaland', 'odisha',
    'punjab', 'rajasthan', 'sikkim', 'tamil nadu', 'telangana', 'tripura',
    'uttar pradesh', 'uttarakhand', 'west bengal'
]

VALID_CROPS = [
    'wheat', 'rice', 'maize', 'sugarcane', 'potato', 'tapioca'
]

def extract_states(text):
    text = text.lower()
    found = [s for s in VALID_STATES if s in text]
    return found

def extract_crops(text):
    text = text.lower()
    found = [c for c in VALID_CROPS if c in text]
    return found

def extract_years(text):
    # Match single years or ranges like 2018-2022
    ranges = re.findall(r'(\b(19|20)\d{2})\s*[-–]\s*(\b(19|20)\d{2})', text)
    if ranges:
        start, end = int(ranges[0][0]), int(ranges[0][2])
        return list(range(start, end+1))
    singles = re.findall(r'\b(19|20)\d{2}\b', text)
    return list(map(int, singles)) if singles else None

def extract_last_n_years(text):
    match = re.search(r'last (\d+)', text)
    return int(match.group(1)) if match else None

# ------------------------
# Main parser
# ------------------------

def parse_question(text):
    text_lower = text.lower()
    states = extract_states(text_lower)
    crops = extract_crops(text_lower)
    years = extract_years(text_lower)
    last_n_years = extract_last_n_years(text_lower)

    # ---------- Rainfall comparisons ----------
    if 'rainfall' in text_lower:
        if 'compare' in text_lower or 'difference' in text_lower:
            return compare_avg_rainfall(
                states,
                last_n_years=last_n_years if last_n_years else None
            )
        else:  # single-state average
            return compare_avg_rainfall(states, last_n_years=last_n_years if last_n_years else 3)

    # ---------- Crop production comparisons ----------
    if crops:
        crop = crops[0]  # default to first crop mentioned
        if 'trend' in text_lower or 'growth' in text_lower:
            years_to_use = last_n_years if last_n_years else 10
            results = []
            for state in states:
                trend = production_trend(state, crop, years=years_to_use)
                # optionally add correlation with rainfall if requested
                if 'rainfall' in text_lower:
                    corr = rainfall_correlation(state, crop, years=years_to_use)
                    trend['RainfallCorrelation'] = corr['Correlation']
                results.append(trend)
            return results

        if 'top' in text_lower or 'most produced' in text_lower:
            m = 5
            return top_m_crops(states, crop, m=m, last_n_years=last_n_years)

        if 'highest' in text_lower or 'lowest' in text_lower or 'district' in text_lower:
            return district_high_low_crop(states, crop, specific_year=years[0] if years else None)

        # Compare crop across states
        if 'compare' in text_lower or len(states) > 1:
            results = []
            for state in states:
                top_c = top_m_crops([state], crop, m=1, last_n_years=last_n_years)
                results.extend(top_c)
            return results

    # ---------- Policy suggestions ----------
    if 'policy' in text_lower or 'promote' in text_lower or 'support' in text_lower:
        # Example: compute top 3 data-backed reasons
        reasons = []
        for crop in crops:
            for state in states:
                trend = production_trend(state, crop, years=last_n_years if last_n_years else 5)
                corr = rainfall_correlation(state, crop, years=last_n_years if last_n_years else 5)
                avg_prod = sum([r['Production'] for r in trend['Trend']])/len(trend['Trend']) if trend['Trend'] else 0
                reasons.append({
                    'State': state,
                    'Crop': crop,
                    'AvgProduction': avg_prod,
                    'RainfallCorrelation': corr['Correlation']
                })
        # Sort by AvgProduction descending as a simple heuristic
        reasons_sorted = sorted(reasons, key=lambda x: x['AvgProduction'], reverse=True)
        return reasons_sorted[:3]

    return {"error": "Question pattern not recognized. Please check state/crop names and keywords."}

# ------------------------
# Wrapper function
# ------------------------
def answer_question(text):
    return parse_question(text)
