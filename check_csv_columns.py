import pandas as pd

# Load your crop data
crop_df = pd.read_csv("crop_data.csv")

# Print the column names
print("Columns in crop_data.csv:", crop_df.columns.tolist())
