import os
import pandas as pd
import psycopg2
import gcsfs
import logging

# GCS Configuration
BUCKET_NAME = "indian_stock_analytics"
SHAREHOLDING_DIR = "financial/2025-08-23/shareholding"

# PostgreSQL Configuration
PG_HOST = "localhost"
PG_PORT = "5432"
PG_DATABASE = "kunal.nandwana"
PG_USER = "kunal.nandwana"
PG_PASSWORD = "root"
PG_TABLE = "indian_stock_analytics.shareholding"

# Function to load data into PostgreSQL
def load_to_postgres(dataframe, table_name):
    try:
        connection = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            database=PG_DATABASE,
            user=PG_USER,
            password=PG_PASSWORD
        )
        cursor = connection.cursor()

        # Create insert query dynamically
        for index, row in dataframe.iterrows():
            columns = ', '.join(row.index)
            values = ', '.join([f"'{str(value)}'" for value in row.values])
            insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values});"
            cursor.execute(insert_query)

        connection.commit()
        print(f"Data loaded into {table_name} successfully.")

    except Exception as e:
        print(f"Error loading data into PostgreSQL: {e}")

    finally:
        if connection:
            cursor.close()
            connection.close()

# Main script
if __name__ == "__main__":
    # Dynamically fetch all Parquet files in the shareholding directory
    fs = gcsfs.GCSFileSystem()
    parquet_files = fs.glob(f"{BUCKET_NAME}/{SHAREHOLDING_DIR}/*/*.parquet")

    for file_path in parquet_files:
        gcs_path = f"gs://{file_path}"

        # Print the GCS path being read
        print(f"Reading from GCS path: {gcs_path}")

        # Read Parquet file directly from GCS
        try:
            df = pd.read_parquet(gcs_path)
            # Print the first 5 records of the DataFrame
            print("First 5 records:")
            print(df.head())

        except Exception as e:
            print(f"  ✗ Could not load data from {gcs_path} ({e}), skipping.")
            continue

        # Load DataFrame into PostgreSQL
        load_to_postgres(df, PG_TABLE)

        print(f"Processed {gcs_path}")
