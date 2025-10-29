import pandas as pd

# Load the CSV
df = pd.read_csv("crop_data_long.csv")

# Rename columns to match expected names in the rest of the project
df = df.rename(columns={
    "state_name": "State",
    "district_name": "District",
    "crop_year": "Year",
    "season": "Season",
    "crop": "Crop",
    "area_": "Area",
    "production_": "Production"
})

# Convert numeric columns
df["Area"] = pd.to_numeric(df["Area"], errors="coerce")
df["Production"] = pd.to_numeric(df["Production"], errors="coerce")

# Melt the DataFrame to long format
df_long = pd.melt(
    df,
    id_vars=["State", "District", "Crop", "Season"],
    value_vars=["Area", "Production"],
    var_name="Measure",
    value_name="Value"
)

# Save the long CSV
df_long.to_csv("crop_data_long_fixed.csv", index=False)

print("Converted CSV saved as 'crop_data_long_fixed.csv'")
print(df_long.head())
