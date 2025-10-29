# Project Samarth — Enhanced Prototype

This enhanced package adds **live data fetching from data.gov.in** (with local CSV fallback),
robust crop/state matching (including fuzzy matching), cross-state district comparison,
policy advisory scaffolding, and improved trend/correlation analysis.

## Key files added
- `config.py` — Put your API keys and resource IDs here.
- `data_fetcher.py` — Fetch datasets from data.gov.in with local caching and fallback.
- `matching.py` — Name cleaning and fuzzy matching utilities.
- `analysis.py` — Trend analysis, growth rates, correlation utilities.
- `enhanced_handler.py` — An upgraded `handle_question()` skeleton tying everything together.
- `requirements.txt` — Suggested Python packages.
- `example_run.py` — Small script that shows how to call the main handler.
- `CHANGELOG.md` — Notes about what was added.

## Usage
1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Edit `config.py` to include your API key and resource IDs (already prefilled).
3. Run `python example_run.py` to see an example query flow.
4. Integrate `enhanced_handler.handle_question` into your existing codebase — it falls back to local CSVs
   if live fetching is unavailable.

## Notes
- This environment couldn't call the API while packaging; the code is ready to run locally or on a server
  with internet access.
- Caching folder: `./cache/` — fetched CSVs are stored there to avoid repeated downloads.
