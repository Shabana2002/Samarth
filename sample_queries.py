import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()
BASE = 'http://localhost:5000/api/query'

# List of sample questions for Phase 1 and Phase 2
questions = [
    "Compare the average annual rainfall in Kerala and Tamil Nadu for the last 5 years and list the top 3 crops.",
    "Identify the district in State_X with the highest production of Crop_Z in the most recent year available and compare that with the district with the lowest production of Crop_Z in State_Y?",
    "Analyze the production trend of Crop_Type_C in the Geographic_Region_Y over the last decade. Correlate this trend with the corresponding climate data for the same period and provide a summary of the apparent impact.",
    "A policy advisor is proposing a scheme to promote Crop_Type_A (e.g., drought-resistant) over Crop_Type_B (e.g., water-intensive) in Geographic_Region_Y. Based on historical data from the last N years, what are the three most compelling data-backed arguments to support this policy?"
]

def ask_question(q: str):
    print("\n" + "="*80)
    print(f"Asking: {q}\n")
    try:
        r = requests.post(BASE, json={'question': q}, timeout=60)
        r.raise_for_status()
        data = r.json()

        # Print JSON nicely
        print("Raw JSON response:")
        print(json.dumps(data, indent=2))

        # Print HTML-friendly answer
        print("\nFormatted Answer:")
        answer_html = data.get('answer_html')
        if answer_html:
            print(answer_html)
        else:
            print("No formatted answer returned.")

        # Print citations
        citations = data.get('citations', [])
        if citations:
            print("\nSources / Citations:")
            for c in citations:
                print(f"- {c['title']}: {c['url']}")
    except Exception as e:
        print("Error calling local app:", e)

if __name__ == "__main__":
    for q in questions:
        ask_question(q)
