import pytest
import sys
import os

# Para importar etl.py desde lambda/
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../lambda"))

from etl import transform

# Data simulada igual a la que devuelve CoinGecko
MOCK_API_RESPONSE = [
    {
        "id": "bitcoin",
        "symbol": "btc",
        "name": "Bitcoin",
        "current_price": 65000.50,
        "market_cap": 1280000000000,
        "market_cap_rank": 1,
        "total_volume": 25000000000,
        "high_24h": 66000.00,
        "low_24h": 64000.00,
        "price_change_24h": 500.25,
        "price_change_percentage_24h": 0.775,
        "circulating_supply": 19700000.0,
        "ath": 73750.0,
    },
    {
        "id": "ethereum",
        "symbol": "eth",
        "name": "Ethereum",
        "current_price": 3500.00,
        "market_cap": 420000000000,
        "market_cap_rank": 2,
        "total_volume": 15000000000,
        "high_24h": 3600.00,
        "low_24h": 3400.00,
        "price_change_24h": -50.00,
        "price_change_percentage_24h": None,  # caso borde: None
        "circulating_supply": 120000000.0,
        "ath": 4878.26,
    },
]


def test_transform_returns_correct_number_of_records():
    result = transform(MOCK_API_RESPONSE, "2026-03-20")
    assert len(result) == 2


def test_transform_symbol_is_uppercase():
    result = transform(MOCK_API_RESPONSE, "2026-03-20")
    assert result[0]["symbol"] == "BTC"
    assert result[1]["symbol"] == "ETH"


def test_transform_adds_ingestion_date():
    result = transform(MOCK_API_RESPONSE, "2026-03-20")
    assert result[0]["ingestion_date"] == "2026-03-20"
    assert result[1]["ingestion_date"] == "2026-03-20"


def test_transform_handles_none_price_change():
    """Verifica que None no rompe el pipeline."""
    result = transform(MOCK_API_RESPONSE, "2026-03-20")
    assert result[1]["price_change_percentage_24h"] == 0.0


def test_transform_required_fields_exist():
    """Verifica que todos los campos necesarios están presentes."""
    required_fields = [
        "id", "symbol", "name", "current_price", "market_cap",
        "market_cap_rank", "total_volume", "high_24h", "low_24h",
        "price_change_24h", "price_change_percentage_24h",
        "circulating_supply", "ath", "ingestion_date"
    ]
    result = transform(MOCK_API_RESPONSE, "2026-03-20")
    for field in required_fields:
        assert field in result[0], f"Campo faltante: {field}"


def test_transform_price_change_is_rounded():
    result = transform(MOCK_API_RESPONSE, "2026-03-20")
    # Verifica que price_change_percentage_24h tiene máximo 2 decimales
    value = result[0]["price_change_percentage_24h"]
    assert value == round(value, 2)