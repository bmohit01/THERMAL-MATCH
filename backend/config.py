"""
ThermalMatch Backend Configuration.
Defines AWS infrastructure parameters, thermodynamic constants, and Gemini AI credentials.
"""

import os
from pydantic import BaseModel

class Settings(BaseModel):
    # App Settings
    APP_NAME: str = "ThermalMatch"
    APP_VERSION: str = "2.4.0-enterprise"
    ENVIRONMENT: str = os.getenv("ENVIRONMENT", "production")
    DEBUG: bool = os.getenv("DEBUG", "false").lower() == "true"
    HOST: str = os.getenv("HOST", "127.0.0.1")
    PORT: int = int(os.getenv("PORT", "8000"))

    # Google Gemini AI Assistant Authentication
    GEMINI_API_ID: str = os.getenv(
        "GEMINI_API_ID",
        os.getenv("GEMINI_API_KEY", "")
    )
    GEMINI_MODEL: str = "gemini-3.8-flash"
    GEMINI_API_ENDPOINT: str = "https://generativelanguage.googleapis.com/v1beta/models"

    # Amazon Kinesis Data Streams Configuration
    AWS_REGION: str = os.getenv("AWS_REGION", "us-east-1")
    KINESIS_STREAM_NAME: str = os.getenv("KINESIS_STREAM_NAME", "thermalmatch-exhaust-telemetry")
    KINESIS_SHARD_COUNT: int = 4
    KINESIS_RETENTION_HOURS: int = 48
    KINESIS_BATCH_SIZE: int = 100
    KINESIS_STREAM_ARN: str = f"arn:aws:kinesis:us-east-1:123456789012:stream/{KINESIS_STREAM_NAME}"

    # Amazon QuickSight Configuration
    QUICKSIGHT_ACCOUNT_ID: str = os.getenv("AWS_ACCOUNT_ID", "123456789012")
    QUICKSIGHT_DASHBOARD_ID: str = os.getenv("QUICKSIGHT_DASHBOARD_ID", "tm-district-analytics-v2")
    QUICKSIGHT_ROLE_ARN: str = f"arn:aws:iam::{QUICKSIGHT_ACCOUNT_ID}:role/ThermalMatchQuickSightEmbedRole"
    QUICKSIGHT_SESSION_LIFETIME_MINUTES: int = 600
    QUICKSIGHT_NAMESPACE: str = "default"

    # Thermodynamic & Economic Baseline Constants (INR & Metric Standards)
    CURRENCY_SYMBOL: str = "₹"
    USD_TO_INR_RATE: float = 84.0
    AMBIENT_GROUND_TEMP_C: float = 10.0      # Average underground soil temperature
    AMBIENT_AIR_TEMP_C: float = 15.0         # Reference ambient air temperature
    DEFAULT_PINCH_DELTA_T: float = 5.0       # Minimum pinch temperature difference (°C)
    INSULATION_CONDUCTIVITY_PUR: float = 0.024 # W/(m·K) Polyurethane foam (EN 253)
    CASING_OUTER_DIAMETER_RATIO: float = 1.45  # Ratio of PE casing to steel pipe
    EXCAVATION_COST_PER_METER: float = 850.0   # Capex for trenching and pipe laying ($/m)
    EXCAVATION_COST_PER_METER_INR: float = 71400.0 # Capex for trenching and pipe laying (₹/m)
    NATURAL_GAS_DISPLACEMENT_COST_PER_MWH: float = 68.50 # Commercial heating replacement cost ($/MWh)
    NATURAL_GAS_DISPLACEMENT_COST_PER_MWH_INR: float = 5754.0 # Commercial heating replacement cost (₹/MWh)
    CARBON_TAX_CREDIT_PER_TONNE: float = 75.00 # $ per metric ton of avoided CO2
    CARBON_TAX_CREDIT_PER_TONNE_INR: float = 6300.0 # ₹ per metric ton of avoided CO2
    GRID_ELECTRICITY_PRICE_PER_KWH: float = 0.14 # Pumping electricity cost ($/kWh)
    GRID_ELECTRICITY_PRICE_PER_KWH_INR: float = 11.75 # Pumping electricity cost (₹/kWh)
    BOILER_EFFICIENCY_BASELINE: float = 0.82   # Displaced natural gas boiler efficiency
    GAS_EMISSION_FACTOR_KG_PER_MWH: float = 202.0 # kg CO2 per MWh of natural gas heat

settings = Settings()
