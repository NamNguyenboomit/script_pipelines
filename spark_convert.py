from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pandas as pd
import glob
import os

spark = (
    SparkSession
        .builder
        .appName("SparkProcessingData")
        .config("spark.dynamicAllocation.enables", "false")
        .config("spark.sql.adaptive.enabled", "false")

        .getOrCreate()
)
sc = spark.sparkContext


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

        # check for serial number date
        if "Sale Dy" in df.columns:
            # pd.to_datetime with unit='D' and the Excel origin fixes 43566 -> 2019-04-11
            # We use errors='coerce' just in case there is some text in the date column
            df["Sale Dy"] = pd.to_datetime(df["Sale Dy"], unit='D', origin='1899-12-30').dt.date

        
        # Add source tracking (good for Iceberg auditing later)
        df["source_file"] = file_name
        
        # 2. Save to Parquet
        df.to_parquet(target_path, engine='pyarrow', index=False)
        print(f"Converted: {file_name} -> {target_name}")


def read_parquet_file(folder_path, df):
    # Define schema for data
    schema = StructType([
        StructField("Store", StringType(), True),
        StructField("Cust No", StringType(), True),
        StructField("Cust Card No", StringType(), True),
        StructField("Cust Type", StringType(), True),
        StructField("Remark", StringType(), True),
        StructField("Card Online Fg", StringType(), True),
        StructField("Sale Dy", StringType(), True),
        StructField("Pos No", StringType(), True),
        StructField("Trd No", StringType(), True),
        StructField("Cust Nm", StringType(), True),
        StructField("Prod Cd", StringType(), True),
        StructField("Srcmk Cd", StringType(), True),
        StructField("Prod Nm", StringType(), True),
        StructField("Prod Type", StringType(), True),
        StructField("L1 Cd", StringType(), True),
        StructField("L1 Nm", StringType(), True),
        StructField("L2 Cd", StringType(), True),
        StructField("L2 Nm", StringType(), True),
        StructField("L3 Cd", StringType(), True),
        StructField("L3 Nm", StringType(), True),
        StructField("L4 Cd", StringType(), True),
        StructField("L4 Nm", StringType(), True),
        StructField("Buy Amt", LongType(), True),
        StructField("Sale Prc", LongType(), True),
        StructField("Sale Qty", IntegerType(), True),
        StructField("Sale Amt", LongType(), True),
        StructField("Profit Rt", DoubleType(), True),
        StructField("Profit Amt", LongType(), True),
        StructField("Vat", LongType(), True),
        StructField("Disc", LongType(), True),
        StructField("Net Sale", LongType(), True),
        StructField("Order ID", StringType(), True),
        StructField("Return", StringType(), True)
    ])
    
    # read file
    try:  
        df = (
            spark
                .read
                .schema(schema)
                .parquet(folder_path)
        )
        print(f"Successfully merged data. Total rows: {df.count()}")

        # Convert the String to Date with your fixed format
        # This will handle "2024-03-16" or "2024/03/16" depending on the format string
        df = df.withColumn("Sale Dy", to_date(col("Sale Dy"), "yyyy-MM-dd"))
        return df
    except Exception as e:
        print(f"Error reading parquets: {e}")
        return None
    
def column_table_stats(df, columnName):
    new_df = (
        df
            # Create Parition ID for column
            .withColumn("Partition Number", spark_partition_id())

            # Calculate statistic of this column
            .groupBy("Partition Number")
            .agg(
                count("*").alias("Record Count"),
                min(columnName).alias("Min Value"),
                max(columnName).alias("Max Value")
            )
            .orderBy("Partition Number")
    )

    return new_df




folder_path = "D:/LotteMart/Pipelines/data/raw"
output_folder = "D:/LotteMart/Pipelines/data/silver"

convert_to_parquet(folder_path, output_folder)