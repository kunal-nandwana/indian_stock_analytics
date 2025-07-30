import pandas as pd

# Load Parquet file
df = pd.read_parquet("daily_2025-07-22_HDFC.parquet")

# Show top rows
print(df.head())

# Check data types
print(df.dtypes)

# Basic stats
print(df.describe(include='all'))

# Check for missing values
print(df.isnull().sum())
