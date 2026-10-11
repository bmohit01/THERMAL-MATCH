"""
ThermalMatch Amazon Kinesis Data Streams Pipeline.
Implements high-throughput telemetry ingestion, partition key routing,
sharding topology, sliding-window stream processing, and thermodynamic anomaly detection.
"""

import asyncio
import hashlib
import json
import math
import random
import time
from datetime import datetime
from typing import Dict, List, Optional
from collections import deque

from backend.config import settings
from backend.models import TelemetryRecord, ShardStatus

class KinesisShard:
    """Represents an active Amazon Kinesis Data Streams shard."""
    def __init__(self, shard_id: str, hash_key_range_start: int, hash_key_range_end: int):
        self.shard_id = shard_id
        self.hash_key_range_start = hash_key_range_start
        self.hash_key_range_end = hash_key_range_end
        self.records_buffer = deque(maxlen=1000)
        self.total_records_processed = 0
        self.bytes_processed = 0
        self.latest_temp_c = 0.0
        self.latest_power_mwt = 0.0
        self.anomalies = []

    def put_record(self, record: TelemetryRecord):
        self.records_buffer.append(record)
        self.total_records_processed += 1
        record_bytes = len(record.model_dump_json())
        self.bytes_processed += record_bytes
        self.latest_temp_c = record.exhaust_temp_c
        self.latest_power_mwt = record.thermal_power_mwt

    def get_windowed_average_temp(self, count: int = 20) -> float:
        if not self.records_buffer:
            return 0.0
        sample = list(self.records_buffer)[-count:]
        return sum(r.exhaust_temp_c for r in sample) / len(sample)

class AmazonKinesisStreamManager:
    """
    Production-grade Amazon Kinesis Data Streams abstraction.
    Manages sharded stream topology, partition hashing, stream consumers,
    and thermodynamic anomaly detection.
    """
    def __init__(self):
        self.stream_name = settings.KINESIS_STREAM_NAME
        self.shards: Dict[str, KinesisShard] = {}
        self.anomaly_log: deque = deque(maxlen=200)
        self.latest_facility_telemetry: Dict[str, TelemetryRecord] = {}
        self._sequence_counter = 100000
        self._initialize_shards()
        self.simulation_running = False

    def _initialize_shards(self):
        """Initializes 4 distributed Amazon Kinesis shards spanning the 128-bit MD5 hash space."""
        total_space = 2**128
        chunk = total_space // settings.KINESIS_SHARD_COUNT
        for i in range(settings.KINESIS_SHARD_COUNT):
            shard_id = f"shardId-{str(i).zfill(12)}"
            start = i * chunk
            end = ((i + 1) * chunk) - 1 if i < settings.KINESIS_SHARD_COUNT - 1 else total_space - 1
            self.shards[shard_id] = KinesisShard(shard_id, start, end)

    def _route_partition_key(self, partition_key: str) -> str:
        """Computes MD5 hash of partition key to route telemetry to designated Kinesis shard."""
        hash_digest = hashlib.md5(partition_key.encode("utf-8")).hexdigest()
        hash_int = int(hash_digest, 16)
        for shard_id, shard in self.shards.items():
            if shard.hash_key_range_start <= hash_int <= shard.hash_key_range_end:
                return shard_id
        return list(self.shards.keys())[0]

    def put_record(self, facility_id: str, facility_name: str, exhaust_temp_c: float,
                   mass_flow_rate_kg_s: float, fluid_medium: str, pressure_bar: float,
                   ambient_temp_c: float = 15.0) -> TelemetryRecord:
        """Ingests a single SCADA exhaust telemetry record into Kinesis Data Streams."""
        shard_id = self._route_partition_key(facility_id)
        self._sequence_counter += 1
        seq_num = f"{int(time.time()*1000)}-{self._sequence_counter}"

        # Calculate thermodynamic enthalpy power (MWt): Q = m_dot * c_p * delta_T
        cp_lookup = {
            "Hot Water": 4.184,
            "Pressurized High-Temp Water": 4.22,
            "Low-Pressure Saturated Steam": 2.10,
            "Clean Flue Gas / Exhaust Air": 1.05,
            "Synthetic Thermal Fluid (Dowtherm)": 2.15,
        }
        cp = cp_lookup.get(fluid_medium, 4.184)
        delta_t = max(0.0, exhaust_temp_c - ambient_temp_c)
        power_mwt = round((mass_flow_rate_kg_s * cp * delta_t) / 1000.0, 3)

        record = TelemetryRecord(
            shard_id=shard_id,
            sequence_number=seq_num,
            facility_id=facility_id,
            facility_name=facility_name,
            exhaust_temp_c=round(exhaust_temp_c, 2),
            mass_flow_rate_kg_s=round(mass_flow_rate_kg_s, 2),
            fluid_medium=fluid_medium,
            pressure_bar=round(pressure_bar, 2),
            ambient_temp_c=round(ambient_temp_c, 2),
            timestamp=datetime.utcnow(),
            thermal_power_mwt=power_mwt
        )

        # Ingest to shard
        target_shard = self.shards[shard_id]
        
        # Anomaly Detection before pushing
        prev_record = self.latest_facility_telemetry.get(facility_id)
        if prev_record:
            temp_diff = abs(record.exhaust_temp_c - prev_record.exhaust_temp_c)
            if temp_diff > 12.0:
                anomaly_entry = {
                    "timestamp": datetime.utcnow().isoformat(),
                    "facility_id": facility_id,
                    "facility_name": facility_name,
                    "shard_id": shard_id,
                    "type": "THERMAL_SHOCK_DRIFT",
                    "previous_temp": prev_record.exhaust_temp_c,
                    "current_temp": record.exhaust_temp_c,
                    "drift_delta": round(temp_diff, 2),
                    "severity": "CRITICAL" if temp_diff > 20.0 else "WARNING"
                }
                self.anomaly_log.append(anomaly_entry)
                target_shard.anomalies.append(anomaly_entry)

        target_shard.put_record(record)
        self.latest_facility_telemetry[facility_id] = record
        return record

    def put_records_batch(self, records_data: List[dict]) -> List[TelemetryRecord]:
        """High-throughput batch ingest mimicking Amazon Kinesis PutRecords API."""
        ingested = []
        for item in records_data:
            rec = self.put_record(
                facility_id=item["facility_id"],
                facility_name=item["facility_name"],
                exhaust_temp_c=item["exhaust_temp_c"],
                mass_flow_rate_kg_s=item["mass_flow_rate_kg_s"],
                fluid_medium=item.get("fluid_medium", "Hot Water"),
                pressure_bar=item.get("pressure_bar", 4.5),
                ambient_temp_c=item.get("ambient_temp_c", 15.0)
            )
            ingested.append(rec)
        return ingested

    def get_shard_statuses(self) -> List[ShardStatus]:
        """Returns health, throughput, and latest temperature for all active Kinesis shards."""
        statuses = []
        for shard_id, shard in self.shards.items():
            throughput = round((shard.bytes_processed / 1024.0) / max(1.0, time.time() % 3600), 2)
            statuses.append(ShardStatus(
                shard_id=shard_id,
                records_processed=shard.total_records_processed,
                current_temp_c=round(shard.latest_temp_c, 1),
                thermal_power_mwt=round(shard.latest_power_mwt, 2),
                throughput_kb_s=throughput,
                health="HEALTHY" if not shard.anomalies or len(shard.anomalies) < 3 else "ATTENTION"
            ))
        return statuses

    def get_latest_telemetry(self, facility_id: Optional[str] = None):
        """Returns the most recent SCADA telemetry record for a facility or all facilities."""
        if facility_id:
            return self.latest_facility_telemetry.get(facility_id)
        return list(self.latest_facility_telemetry.values())

    def get_anomalies(self, limit: int = 20) -> List[dict]:
        """Returns recent thermal anomalies detected on the stream."""
        return list(self.anomaly_log)[-limit:]

# Singleton instance
kinesis_stream = AmazonKinesisStreamManager()
