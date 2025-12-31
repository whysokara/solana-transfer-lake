import requests
import csv
from datetime import datetime

API_KEY = "fe0265e7-67d2-4649-a733-9f3383fc9a3d"

URL = f"https://api.helius.xyz/v0/addresses/So11111111111111111111111111111111111111112/transactions?api-key={API_KEY}"

params = {
    "limit": 100
}

response = requests.get(URL, params=params)
response.raise_for_status()

data = response.json()

output_path = "data/raw/transfers/date=2025-12-31/transfers.csv"

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
