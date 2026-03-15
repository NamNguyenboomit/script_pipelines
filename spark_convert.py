from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType
import pandas as pd
import glob
import os

def convert_to_parquet(folder_path, output_folder):
    # 1. Create output folder if it doesn't exist
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    csv_files = glob.glob(os.path.join(folder_path, "*.csv"))
    excel_files = glob.glob(os.path.join(folder_path, "*.xlsx"))
    all_files = csv_files + excel_files

    if not all_files:
        print("No files found.")
        return

    for file_path in all_files:
        file_name = os.path.basename(file_path)
        # Create a new filename (e.g., data.csv -> data.parquet)
        target_name = os.path.splitext(file_name)[0] + ".parquet"
        target_path = os.path.join(output_folder, target_name)

        # Read based on extension
        if file_path.endswith(".csv"):
            df = pd.read_csv(file_path)
        else:
            df = pd.read_excel(file_path)

        # Add source tracking (good for Iceberg auditing later)
        df["source_file"] = file_name
        
        # 2. Save to Parquet
        df.to_parquet(target_path, engine='pyarrow', index=False)
        print(f"Converted: {file_name} -> {target_name}")

folder_path = "D:/LotteMart/Pipelines/data/raw"
output_folder = "D:/LotteMart/Pipelines/data/silver"

convert_to_parquet(folder_path, output_folder)