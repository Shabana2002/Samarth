import pandas as pd

rain_df = pd.read_csv("rainfall_data.csv")

# Strip extra spaces from the 'State' column
rain_df['State'] = rain_df['State'].astype(str).str.strip()

# See all unique states
print(rain_df['State'].unique())
