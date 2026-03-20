import json
import boto3
import urllib.request
import csv
import io
import os
from datetime import datetime, timezone

s3 = boto3.client("s3")


def fetch_crypto_data() -> list[dict]:
    """Llama a CoinGecko API y retorna top 100 cryptos."""
    url = (
        "https://api.coingecko.com/api/v3/coins/markets"
        "?vs_currency=usd&order=market_cap_desc&per_page=100&page=1"
    )
    req = urllib.request.Request(url, headers={"User-Agent": "ETL-Pipeline/1.0"})
    with urllib.request.urlopen(req, timeout=30) as response:
        return json.loads(response.read())


def save_raw(data: list[dict], date_str: str) -> str:
    """Guarda JSON crudo en S3 particionado por fecha."""
    RAW_BUCKET = os.environ["RAW_BUCKET"]  # ← movido aquí
    key = f"crypto/year={date_str[:4]}/month={date_str[5:7]}/day={date_str[8:10]}/raw_{date_str}.json"
    s3.put_object(
        Bucket=RAW_BUCKET,
        Key=key,
        Body=json.dumps(data, indent=2),
        ContentType="application/json",
    )
    print(f"Raw data saved: s3://{RAW_BUCKET}/{key}")
    return key


def transform(data: list[dict], date_str: str) -> list[dict]:
    """Limpia y transforma los datos."""
    transformed = []
    for coin in data:
        transformed.append({
            "id":                          coin.get("id", ""),
            "symbol":                      coin.get("symbol", "").upper(),
            "name":                        coin.get("name", ""),
            "current_price":               coin.get("current_price", 0),
            "market_cap":                  coin.get("market_cap", 0),
            "market_cap_rank":             coin.get("market_cap_rank", 0),
            "total_volume":                coin.get("total_volume", 0),
            "high_24h":                    coin.get("high_24h", 0),
            "low_24h":                     coin.get("low_24h", 0),
            "price_change_24h":            round(coin.get("price_change_24h") or 0, 4),
            "price_change_percentage_24h": round(coin.get("price_change_percentage_24h") or 0, 2),
            "circulating_supply":          coin.get("circulating_supply", 0),
            "ath":                         coin.get("ath", 0),
            "ingestion_date":              date_str,
        })
    return transformed


def save_processed(data: list[dict], date_str: str) -> str:
    """Guarda CSV procesado en S3 particionado por fecha."""
    PROCESSED_BUCKET = os.environ["PROCESSED_BUCKET"]  # ← movido aquí
    if not data:
        return ""

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=data[0].keys())
    writer.writeheader()
    writer.writerows(data)

    key = f"crypto/year={date_str[:4]}/month={date_str[5:7]}/day={date_str[8:10]}/processed_{date_str}.csv"
    s3.put_object(
        Bucket=PROCESSED_BUCKET,
        Key=key,
        Body=output.getvalue(),
        ContentType="text/csv",
    )
    print(f"Processed data saved: s3://{PROCESSED_BUCKET}/{key}")
    return key


def lambda_handler(event, context):
    """Entry point de Lambda."""
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    print(f"Starting ETL pipeline for date: {date_str}")

    try:
        print("Extracting data from CoinGecko...")
        raw_data = fetch_crypto_data()
        print(f"Fetched {len(raw_data)} coins")

        raw_key = save_raw(raw_data, date_str)

        print("Transforming data...")
        processed_data = transform(raw_data, date_str)

        processed_key = save_processed(processed_data, date_str)

        return {
            "statusCode": 200,
            "body": {
                "message": "ETL completado exitosamente",
                "date": date_str,
                "records_processed": len(processed_data),
                "raw_key": raw_key,
                "processed_key": processed_key,
            }
        }

    except Exception as e:
        print(f"ERROR en el pipeline: {str(e)}")
        raise e