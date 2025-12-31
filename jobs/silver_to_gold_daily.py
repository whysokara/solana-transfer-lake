import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)

import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    count,
    sum as _sum,
    countDistinct,
    col
)

def parse_args():
    parser = argparse.ArgumentParser(description="Solana transfers silver → gold daily aggregates")
    parser.add_argument(
        "--date",
        required=True,
        help="Ingestion date in YYYY-MM-DD format"
    )
    return parser.parse_args()


def main():
    args = parse_args()
    date = args.date

    silver_path = f"data/silver/transfers/date={date}"
    gold_path = f"data/gold/transfers_daily/date={date}"

    if not os.path.exists(silver_path):
        raise FileNotFoundError(f"Silver data not found: {silver_path}")

    spark = (
        SparkSession.builder
        .appName("solana_silver_to_gold_daily")
        .getOrCreate()
    )

    df = spark.read.parquet(silver_path)

    df_gold = (
        df
        .groupBy("event_date")
        .agg(
            count("*").alias("total_transactions"),
            _sum("amount").alias("total_amount"),
            countDistinct("from_address").alias("unique_senders"),
            countDistinct("to_address").alias("unique_receivers")
        )
        .select(
            col("event_date"),
            col("total_transactions"),
            col("total_amount"),
            col("unique_senders"),
            col("unique_receivers")
        )
    )

    logger.info("Gold dataframe schema:")
    df_gold.printSchema()

    logger.info("Gold daily aggregates:")
    df_gold.show(truncate=False)

    logger.info(f"Writing gold data to {gold_path}")


    (
        df_gold
        .write
        .mode("overwrite")
        .parquet(gold_path)
    )

    logger.info("Gold write completed successfully")

    gold_count = df_gold.count()
    logger.info(f"Gold row count: {gold_count}")

    if gold_count == 0:
        raise ValueError("Gold dataset is empty. Failing job.")

    spark.stop()


if __name__ == "__main__":
    main()
