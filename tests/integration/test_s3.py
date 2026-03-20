import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../lambda"))

from etl import transform

MOCK_DATA = [
    {
        "id": "bitcoin", "symbol": "btc", "name": "Bitcoin",
        "current_price": 65000, "market_cap": 1280000000000,
        "market_cap_rank": 1, "total_volume": 25000000000,
        "high_24h": 66000, "low_24h": 64000,
        "price_change_24h": 500, "price_change_percentage_24h": 0.77,
        "circulating_supply": 19700000, "ath": 73750,
    },
    {
        "id": "ethereum", "symbol": "eth", "name": "Ethereum",
        "current_price": 3500, "market_cap": 420000000000,
        "market_cap_rank": 2, "total_volume": 15000000000,
        "high_24h": 3600, "low_24h": 3400,
        "price_change_24h": -50, "price_change_percentage_24h": -1.4,
        "circulating_supply": 120000000, "ath": 4878,
    },
]


def test_no_duplicate_ids():
    """No debe haber cryptos duplicadas."""
    result = transform(MOCK_DATA, "2026-03-20")
    ids = [r["id"] for r in result]
    assert len(ids) == len(set(ids)), "Hay IDs duplicados"


def test_prices_are_positive():
    """Todos los precios deben ser positivos."""
    result = transform(MOCK_DATA, "2026-03-20")
    for coin in result:
        assert coin["current_price"] > 0, f"{coin['name']} tiene precio negativo"
        assert coin["market_cap"] > 0, f"{coin['name']} tiene market cap negativo"


def test_market_cap_rank_is_positive_integer():
    """El ranking debe ser un entero positivo."""
    result = transform(MOCK_DATA, "2026-03-20")
    for coin in result:
        assert isinstance(coin["market_cap_rank"], int)
        assert coin["market_cap_rank"] > 0


def test_high_24h_greater_than_low_24h():
    """El precio máximo debe ser mayor al mínimo."""
    result = transform(MOCK_DATA, "2026-03-20")
    for coin in result:
        assert coin["high_24h"] >= coin["low_24h"], \
            f"{coin['name']}: high_24h < low_24h"


def test_ingestion_date_format():
    """La fecha debe tener formato YYYY-MM-DD."""
    result = transform(MOCK_DATA, "2026-03-20")
    import re
    pattern = r"^\d{4}-\d{2}-\d{2}$"
    for coin in result:
        assert re.match(pattern, coin["ingestion_date"]), \
            f"Fecha con formato incorrecto: {coin['ingestion_date']}"


def test_no_null_critical_fields():
    """Los campos críticos no deben ser None o vacíos."""
    result = transform(MOCK_DATA, "2026-03-20")
    critical_fields = ["id", "symbol", "name", "current_price", "market_cap"]
    for coin in result:
        for field in critical_fields:
            assert coin[field] is not None, f"{field} es None en {coin['name']}"
            assert coin[field] != "", f"{field} está vacío en {coin['name']}"