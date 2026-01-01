import requests
import csv
import os
import argparse
from datetime import datetime


def parse_args():
    parser = argparse.ArgumentParser(
        description="Fetch Solana transfer data"
    )
    parser.add_argument(
        "--date",
        required=True,
        help="Ingestion date in YYYY-MM-DD format"
    )
    return parser.parse_args()


API_KEY = "fe0265e7-67d2-4649-a733-9f3383fc9a3d"

URL = (
    "https://api.helius.xyz/v0/addresses/"
    "So11111111111111111111111111111111111111112/transactions"
)

PARAMS = {
    "limit": 100
}


def main():
    args = parse_args()
    date = args.date

    response = requests.get(
        URL,
        params={**PARAMS, "api-key": API_KEY}
    )
    response.raise_for_status()

    data = response.json()

    raw_dir = f"data/raw/transfers/date={date}"
    os.makedirs(raw_dir, exist_ok=True)

    output_path = os.path.join(raw_dir, "transfers.csv")

    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "signature",
            "timestamp",
            "from_address",
            "to_address",
            "amount",
            "token_mint",
            "fee",
            "success"
        ])

        for tx in data:
            if "tokenTransfers" not in tx:
                continue

            for t in tx["tokenTransfers"]:
                writer.writerow([
                    tx.get("signature"),
                    datetime.utcfromtimestamp(tx.get("timestamp")).isoformat(),
                    t.get("fromUserAccount"),
                    t.get("toUserAccount"),
                    t.get("tokenAmount"),
                    t.get("mint"),
                    tx.get("fee"),
                    tx.get("transactionError") is None
                ])


if __name__ == "__main__":
    main()
