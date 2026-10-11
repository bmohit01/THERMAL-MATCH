"""
ThermalMatch Data Models & Schemas.
Provides strict Pydantic definitions for industrial telemetry, routing matches,
QuickSight authentication payloads, and Gemini AI queries.
"""

from enum import Enum
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from datetime import datetime

class FacilityType(str, Enum):
    PRODUCER = "PRODUCER"
    CONSUMER = "CONSUMER"

class IndustrySector(str, Enum):
    DATA_CENTER = "HyperScale Data Center"
    FOUNDRY = "Metallurgical Foundry"
    GLASS_MANUFACTURING = "Precision Glass Works"
    CHEMICAL_PROCESSING = "Petrochemical Processing"
    AGRICULTURAL_DRYING = "Agricultural Grain & Produce Drying"
    DISTRICT_HEATING = "Municipal District Thermal Network"
    PAPER_PULP = "Paper & Packaging Pulping"
    FOOD_BEVERAGE = "Industrial Brewing & Distillation"

class FluidMedium(str, Enum):
    WATER = "Hot Water"
    PRESSURIZED_WATER = "Pressurized High-Temp Water"
    STEAM = "Low-Pressure Saturated Steam"
    FLUE_GAS = "Clean Flue Gas / Exhaust Air"
    THERMAL_OIL = "Synthetic Thermal Fluid (Dowtherm)"

class Facility(BaseModel):
    id: str = ""
    name: str
    facility_type: FacilityType
    sector: IndustrySector
    latitude: float
    longitude: float
    zone_id: str = "zone-regional-corridor"
    temperature_c: float = Field(..., description="Exhaust temp for producer or required temp for consumer")
    mass_flow_rate_kg_s: float
    fluid_medium: FluidMedium
    thermal_capacity_mwt: float = Field(..., description="Thermal megawatts available or demanded")
    operating_hours_year: int = 8000
    description: str = ""
    current_status: str = "ONLINE"

class TelemetryRecord(BaseModel):
    shard_id: str
    sequence_number: str
    facility_id: str
    facility_name: str
    exhaust_temp_c: float
    mass_flow_rate_kg_s: float
    fluid_medium: str
    pressure_bar: float
    ambient_temp_c: float
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    thermal_power_mwt: float

class TelemetryBatchIngest(BaseModel):
    records: List[TelemetryRecord]

class ShardStatus(BaseModel):
    shard_id: str
    records_processed: int
    current_temp_c: float
    thermal_power_mwt: float
    throughput_kb_s: float
    health: str = "HEALTHY"

class RoutingMatch(BaseModel):
    match_id: str
    producer_id: str
    producer_name: str
    producer_sector: str
    producer_temp_c: float
    consumer_id: str
    consumer_name: str
    consumer_sector: str
    consumer_required_temp_c: float
    distance_km: float
    pipe_diameter_mm: int
    fluid_velocity_m_s: float
    heat_loss_mwt: float
    delivered_temp_c: float
    pinch_temperature_margin_c: float
    thermal_efficiency_pct: float
    capex_usd: float
    capex_inr: float = 0.0
    opex_usd_yr: float
    opex_inr_yr: float = 0.0
    annual_energy_mwh: float
    financial_savings_usd_yr: float
    financial_savings_inr_yr: float = 0.0
    co2_avoided_tonnes_yr: float
    payback_years: float
    lcoh_usd_mwh: float
    lcoh_inr_mwh: float = 0.0
    feasibility_score: float = Field(..., description="Normalized 0-100 routing match score")
    pipeline_coordinates: List[List[float]] = Field(default_factory=list)

class TopLevelMetrics(BaseModel):
    financial_savings_usd_yr: float
    financial_savings_usd_hr: float
    financial_savings_inr_yr: float = 0.0
    financial_savings_inr_hr: float = 0.0
    emissions_avoided_tonnes_yr: float
    cars_equivalent_removed: int
    active_thermal_power_mwt: float
    network_efficiency_pct: float
    active_producers_count: int
    active_consumers_count: int
    online_kinesis_shards: int
    currency_symbol: str = "₹"
    last_updated: datetime = Field(default_factory=datetime.utcnow)

class QuickSightEmbedRequest(BaseModel):
    dashboard_id: Optional[str] = None
    user_arn: Optional[str] = None
    session_tags: Optional[Dict[str, str]] = None

class QuickSightEmbedResponse(BaseModel):
    embed_url: str
    status: int = 200
    token_expiry_timestamp: str
    dashboard_id: str
    role_arn: str
    session_tags: Dict[str, str]

class GeminiQueryRequest(BaseModel):
    query: str
    zone_id: Optional[str] = "all"
    selected_producer_id: Optional[str] = None
    selected_consumer_id: Optional[str] = None

class GeminiQueryResponse(BaseModel):
    query: str
    advisory_text: str
    executive_summary: str
    recommended_pairings: List[Dict[str, Any]]
    thermodynamic_calculations: Dict[str, Any]
    financial_payback_years: float
    co2_offset_tonnes_yr: float
    api_id_authenticated: str
    context_metadata: Dict[str, Any]
    timestamp: datetime = Field(default_factory=datetime.utcnow)
