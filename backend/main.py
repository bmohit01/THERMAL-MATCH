"""
ThermalMatch Main FastAPI Application.
Hosts REST & streaming endpoints for Kinesis telemetry, thermodynamic routing,
QuickSight secure analytics, and Google Gemini AI Assistant.
"""

import asyncio
import random
from typing import List, Dict, Any, Optional
from datetime import datetime

from fastapi import FastAPI, BackgroundTasks, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from backend.config import settings
from backend.models import (
    Facility, FacilityType, RoutingMatch, TopLevelMetrics, ShardStatus, TelemetryRecord,
    QuickSightEmbedRequest, QuickSightEmbedResponse, GeminiQueryRequest
)
from backend.kinesis_pipeline import kinesis_stream
from backend.thermal_engine import thermal_engine, DEFAULT_FACILITIES
from backend.quicksight_service import quicksight_service
from backend.gemini_service import gemini_assistant

app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    description="Industrial Heat Waste Exchange — Antigravity Platform",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Enable CORS for local testing and AWS Amplify distribution
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Background telemetry generator state
bg_task_handle = None

async def continuous_kinesis_simulator():
    """Generates continuous live SCADA stream oscillations into Kinesis Data Streams shards."""
    producers = thermal_engine.producers
    while True:
        try:
            for prod in producers:
                # Realistic industrial thermal noise
                temp_delta = random.uniform(-0.35, 0.35)
                current_temp = prod.temperature_c + temp_delta
                flow_delta = random.uniform(-0.5, 0.5)
                current_flow = max(10.0, prod.mass_flow_rate_kg_s + flow_delta)

                kinesis_stream.put_record(
                    facility_id=prod.id,
                    facility_name=prod.name,
                    exhaust_temp_c=current_temp,
                    mass_flow_rate_kg_s=current_flow,
                    fluid_medium=prod.fluid_medium.value,
                    pressure_bar=4.8 + random.uniform(-0.1, 0.1),
                    ambient_temp_c=settings.AMBIENT_AIR_TEMP_C + random.uniform(-0.2, 0.2)
                )
            await asyncio.sleep(2.0)
        except Exception:
            await asyncio.sleep(2.0)

@app.on_event("startup")
async def on_startup():
    """Initializes telemetry records and background Kinesis generator."""
    # Seed initial records for all producers
    for prod in thermal_engine.producers:
        kinesis_stream.put_record(
            facility_id=prod.id,
            facility_name=prod.name,
            exhaust_temp_c=prod.temperature_c,
            mass_flow_rate_kg_s=prod.mass_flow_rate_kg_s,
            fluid_medium=prod.fluid_medium.value,
            pressure_bar=4.8
        )
    # Launch background continuous streaming
    asyncio.create_task(continuous_kinesis_simulator())

# =========================================================================
# 1. FACILITY & NETWORK DISCOVERY ENDPOINTS
# =========================================================================

@app.get("/api/facilities", response_model=List[Facility])
async def get_facilities():
    """Returns all registered industrial heat producers and consumers."""
    return thermal_engine.facilities

@app.post("/api/facilities", response_model=Facility)
async def create_facility(facility: Facility):
    """Registers a new industrial heat company facility with location and telemetry parameters."""
    if not facility.id:
        prefix = "PROD" if facility.facility_type == FacilityType.PRODUCER else "CONS"
        count = len(thermal_engine.producers) + 1 if facility.facility_type == FacilityType.PRODUCER else len(thermal_engine.consumers) + 1
        facility.id = f"{prefix}-{str(count).zfill(2)}"

    thermal_engine.add_facility(facility)

    if facility.facility_type == FacilityType.PRODUCER:
        kinesis_stream.put_record(
            facility_id=facility.id,
            facility_name=facility.name,
            exhaust_temp_c=facility.temperature_c,
            mass_flow_rate_kg_s=facility.mass_flow_rate_kg_s,
            fluid_medium=facility.fluid_medium.value,
            pressure_bar=4.8
        )
    return facility

@app.get("/api/summary", response_model=TopLevelMetrics)
async def get_summary_metrics():
    """
    Computes top-level uncluttered metrics for the minimalist dashboard:
    1. Real-time financial savings ($/yr & $/hr)
    2. Carbon emissions avoided (tCO2e/yr)
    3. Active thermal exchange power (MWt)
    4. Network transmission efficiency (%)
    """
    latest_telemetry = kinesis_stream.get_latest_telemetry()
    temp_map = {r.facility_id: r.exhaust_temp_c for r in latest_telemetry}
    matches = thermal_engine.calculate_all_matches(live_telemetry=temp_map)

    # Consider top viable matches for network totals
    active_matches = matches[:5]
    total_savings_yr = sum(m.financial_savings_usd_yr for m in active_matches)
    savings_per_hr = round(total_savings_yr / 8760.0, 2)
    inr_rate = settings.USD_TO_INR_RATE
    total_savings_inr_yr = round(total_savings_yr * inr_rate, 0)
    savings_inr_hr = round(savings_per_hr * inr_rate, 2)
    total_co2 = sum(m.co2_avoided_tonnes_yr for m in active_matches)
    total_mwt = sum(m.heat_loss_mwt + (m.annual_energy_mwh / 8000.0) for m in active_matches)
    avg_eff = round(sum(m.thermal_efficiency_pct for m in active_matches) / max(1, len(active_matches)), 1)

    return TopLevelMetrics(
        financial_savings_usd_yr=round(total_savings_yr, 0),
        financial_savings_usd_hr=savings_per_hr,
        financial_savings_inr_yr=total_savings_inr_yr,
        financial_savings_inr_hr=savings_inr_hr,
        emissions_avoided_tonnes_yr=round(total_co2, 0),
        cars_equivalent_removed=int(total_co2 * 0.22),
        active_thermal_power_mwt=round(total_mwt, 1),
        network_efficiency_pct=avg_eff,
        active_producers_count=len(thermal_engine.producers),
        active_consumers_count=len(thermal_engine.consumers),
        online_kinesis_shards=len(kinesis_stream.shards),
        currency_symbol="₹"
    )

# =========================================================================
# 2. THERMODYNAMIC ROUTING & MATCHING ENDPOINTS
# =========================================================================

@app.get("/api/matches", response_model=List[RoutingMatch])
async def get_routing_matches(limit: int = Query(20, ge=1, le=50)):
    """Computes and ranks all potential thermal pipeline routes based on live Kinesis stream temperatures."""
    latest_telemetry = kinesis_stream.get_latest_telemetry()
    temp_map = {r.facility_id: r.exhaust_temp_c for r in latest_telemetry}
    matches = thermal_engine.calculate_all_matches(live_telemetry=temp_map)
    return matches[:limit]

# =========================================================================
# 3. AMAZON KINESIS DATA STREAMS ENDPOINTS
# =========================================================================

@app.get("/api/kinesis/stream", response_model=List[ShardStatus])
async def get_kinesis_stream_shards():
    """Returns throughput, record count, and health of active Kinesis Data Streams shards."""
    return kinesis_stream.get_shard_statuses()

@app.get("/api/kinesis/telemetry/live", response_model=List[TelemetryRecord])
async def get_live_kinesis_telemetry():
    """Fetches real-time SCADA exhaust temperatures streaming across Kinesis shards."""
    return kinesis_stream.get_latest_telemetry()

@app.post("/api/kinesis/ingest", response_model=TelemetryRecord)
async def ingest_scada_record(
    facility_id: str,
    exhaust_temp_c: float,
    mass_flow_rate_kg_s: float,
    fluid_medium: str = "Hot Water",
    pressure_bar: float = 4.5
):
    """Scaffolds high-throughput streaming ingest endpoint into Amazon Kinesis Data Streams."""
    fac = next((f for f in thermal_engine.producers if f.id == facility_id), None)
    fac_name = fac.name if fac else f"Facility-{facility_id}"
    record = kinesis_stream.put_record(
        facility_id=facility_id,
        facility_name=fac_name,
        exhaust_temp_c=exhaust_temp_c,
        mass_flow_rate_kg_s=mass_flow_rate_kg_s,
        fluid_medium=fluid_medium,
        pressure_bar=pressure_bar
    )
    return record

@app.get("/api/kinesis/anomalies")
async def get_kinesis_anomalies():
    """Returns real-time thermal shock or sensor drift anomalies detected in the stream."""
    return kinesis_stream.get_anomalies()

# =========================================================================
# 4. AMAZON QUICKSIGHT SECURE EMBEDDING ENDPOINTS
# =========================================================================

@app.get("/api/quicksight/embed-url", response_model=QuickSightEmbedResponse)
@app.post("/api/quicksight/embed-url", response_model=QuickSightEmbedResponse)
async def generate_quicksight_embed_url(req: Optional[QuickSightEmbedRequest] = None):
    """
    Generates a secure Amazon QuickSight embed URL signed with STS AssumeRole,
    HMAC authentication tokens, and tenant Row-Level Security (RLS) tags.
    """
    dash_id = req.dashboard_id if req else None
    tags = req.session_tags if req else None
    return quicksight_service.generate_embed_url(dashboard_id=dash_id, session_tags=tags)

@app.get("/api/quicksight/dataset")
async def get_quicksight_dataset():
    """Feeds structured dataset metrics into embedded Amazon QuickSight dashboards."""
    return quicksight_service.get_quicksight_dataset_metrics()

# =========================================================================
# 5. GOOGLE GEMINI AI ASSISTANT ENDPOINT
# =========================================================================

@app.post("/api/gemini/query")
async def query_gemini_assistant(req: GeminiQueryRequest):
    """
    Dispatches facility manager plain-text queries to Google Gemini.
    Gathers live Kinesis data and routing matrix before invoking model.
    Utilizes configured GEMINI_API_ID / GEMINI_API_KEY.
    """
    if not req.query.strip():
        raise HTTPException(status_code=400, detail="Query text cannot be empty.")
    
    response = await gemini_assistant.generate_advisory(req.query, zone_id=req.zone_id)
    return response

# =========================================================================
# 6. STATIC FILES & FRONTEND MOUNTING
# =========================================================================

app.mount("/static", StaticFiles(directory="frontend"), name="static")

@app.get("/")
async def serve_index():
    """Serves the minimalist, user-friendly ThermalMatch frontend."""
    return FileResponse("frontend/index.html")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("backend.main:app", host=settings.HOST, port=settings.PORT, reload=settings.DEBUG)
