import argparse
import os
from pyspark.sql.functions import to_date, col


from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    DecimalType,
    LongType,
    BooleanType
)

def parse_args():
    parser = argparse.ArgumentParser(description="Solana transfers CSV → Parquet")
    parser.add_argument(
        "--date",
        required=True,
        help="Ingestion date in YYYY-MM-DD format"
    )
    return parser.parse_args()


def get_schema():
    return StructType([
        StructField("signature", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("from_address", StringType(), True),
        StructField("to_address", StringType(), True),
        StructField("amount", DecimalType(18, 8), True),
        StructField("token_mint", StringType(), True),
        StructField("fee", LongType(), True),
        StructField("success", BooleanType(), True),
    ])


def main():
    args = parse_args()
    date = args.date

    raw_path = f"data/raw/transfers/date={date}/transfers.csv"
    silver_path = f"data/silver/transfers/date={date}"

    if not os.path.exists(raw_path):
        raise FileNotFoundError(f"Raw file not found: {raw_path}")

    spark = (
        SparkSession.builder
        .appName("solana_csv_to_parquet")
        .getOrCreate()
    )

    schema = get_schema()

    df = (
        spark.read
        .option("header", "true")
        .schema(schema)
        .csv(raw_path)
    )

    # data quality filters
    df_clean = (
    df
    .filter("success = true")
    .filter("amount IS NOT NULL")
    .filter("amount > 0")
    .filter("from_address IS NOT NULL")
    .filter("to_address IS NOT NULL")
    .withColumn("event_date", to_date(col("timestamp")))
)
    df_silver = df_clean.select(
    col("event_date"),
    col("timestamp"),
    col("signature"),
    col("from_address"),
    col("to_address"),
    col("token_mint"),
    col("amount"),
    col("fee")
)


    # Inspect
    df_clean.printSchema()
    df_clean.show(5, truncate=False)

    (
    df_silver
    .write
    .mode("overwrite")
    .parquet(silver_path)
)


    spark.stop()


if __name__ == "__main__":
    main()
