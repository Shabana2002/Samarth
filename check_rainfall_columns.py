import pandas as pd

# Load the rainfall CSV
rainfall_df = pd.read_csv("rainfall_data.csv")

# Print column names
print("Columns in rainfall_data.csv:", rainfall_df.columns.tolist())

# Preview the first 100 rows
print("\nFirst 100 rows of the dataset:")
print(rainfall_df.head(100))

# Preview first 100 rows of selected columns
print("\nFirst 100 rows of selected columns:")
print(rainfall_df[['Year', 'State', 'rainfall']].head(100))
