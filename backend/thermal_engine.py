"""
ThermalMatch Thermodynamic Routing & Matching Engine.
Calculates EN 253 pipeline heat dissipation, hydraulic pumping work,
thermal pinch point feasibility, Levelized Cost of Heat (LCOH), and multi-facility pairing ranking.
"""

import math
from typing import List, Dict, Optional
from backend.config import settings
from backend.models import (
    Facility, FacilityType, IndustrySector, FluidMedium, RoutingMatch
)

# Reference Facilities in the Regional Industrial Exchange Corridor
DEFAULT_FACILITIES: List[Facility] = [
    # Heat Producers in Regional Industrial Corridor
    Facility(
        id="PROD-01",
        name="Company A",
        facility_type=FacilityType.PRODUCER,
        sector=IndustrySector.DATA_CENTER,
        latitude=18.7250,
        longitude=73.7400,
        zone_id="zone-tech-corridor",
        temperature_c=82.0,
        mass_flow_rate_kg_s=42.0,
        fluid_medium=FluidMedium.WATER,
        thermal_capacity_mwt=5.2,
        operating_hours_year=8760,
        description="Continuous liquid-cooling server exhaust with steady low-grade thermal flux in Technology Sector.",
        current_status="ONLINE"
    ),
    Facility(
        id="PROD-02",
        name="Company B",
        facility_type=FacilityType.PRODUCER,
        sector=IndustrySector.FOUNDRY,
        latitude=18.7650,
        longitude=73.8100,
        zone_id="zone-smelting-corridor",
        temperature_c=260.0,
        mass_flow_rate_kg_s=60.0,
        fluid_medium=FluidMedium.STEAM,
        thermal_capacity_mwt=26.4,
        operating_hours_year=7800,
        description="Electric induction arc furnace exhaust and continuous steel billet quenching steam manifold in Smelting Sector.",
        current_status="ONLINE"
    ),
    Facility(
        id="PROD-03",
        name="Company C",
        facility_type=FacilityType.PRODUCER,
        sector=IndustrySector.GLASS_MANUFACTURING,
        latitude=18.7100,
        longitude=73.8350,
        zone_id="zone-manufacturing-corridor",
        temperature_c=195.0,
        mass_flow_rate_kg_s=36.0,
        fluid_medium=FluidMedium.FLUE_GAS,
        thermal_capacity_mwt=11.8,
        operating_hours_year=8200,
        description="Recuperative regenerative glass melting tank clean exhaust flue gas in Manufacturing Hub.",
        current_status="ONLINE"
    ),
    Facility(
        id="PROD-04",
        name="Company D",
        facility_type=FacilityType.PRODUCER,
        sector=IndustrySector.CHEMICAL_PROCESSING,
        latitude=18.7520,
        longitude=73.7150,
        zone_id="zone-chemical-corridor",
        temperature_c=145.0,
        mass_flow_rate_kg_s=85.0,
        fluid_medium=FluidMedium.PRESSURIZED_WATER,
        thermal_capacity_mwt=28.5,
        operating_hours_year=8400,
        description="Fractionation column condenser reboiler rejection line with steady medium-grade enthalpy in Chemical Belt.",
        current_status="ONLINE"
    ),
    Facility(
        id="PROD-05",
        name="Company E",
        facility_type=FacilityType.PRODUCER,
        sector=IndustrySector.FOOD_BEVERAGE,
        latitude=18.6920,
        longitude=73.7300,
        zone_id="zone-agro-corridor",
        temperature_c=94.0,
        mass_flow_rate_kg_s=32.0,
        fluid_medium=FluidMedium.WATER,
        thermal_capacity_mwt=4.8,
        operating_hours_year=7600,
        description="High-enthalpy mash wash heat recovery system with pure industrial condensation water in Agro-Industrial Belt.",
        current_status="ONLINE"
    ),

    # Heat Consumers in Regional Industrial Corridor
    Facility(
        id="CONS-01",
        name="Company F",
        facility_type=FacilityType.CONSUMER,
        sector=IndustrySector.AGRICULTURAL_DRYING,
        latitude=18.7180,
        longitude=73.7250,
        zone_id="zone-agro-corridor",
        temperature_c=62.0,
        mass_flow_rate_kg_s=35.0,
        fluid_medium=FluidMedium.WATER,
        thermal_capacity_mwt=4.5,
        operating_hours_year=8400,
        description="Regional climate-controlled floriculture and winter canopy heating greenhouse facility in Agro Belt.",
        current_status="ACTIVE_DEMAND"
    ),
    Facility(
        id="CONS-02",
        name="Company G",
        facility_type=FacilityType.CONSUMER,
        sector=IndustrySector.AGRICULTURAL_DRYING,
        latitude=18.7280,
        longitude=73.8200,
        zone_id="zone-manufacturing-corridor",
        temperature_c=74.0,
        mass_flow_rate_kg_s=28.0,
        fluid_medium=FluidMedium.WATER,
        thermal_capacity_mwt=3.8,
        operating_hours_year=7200,
        description="Continuous fluidized-bed agricultural grain, seed, and turmeric moisture reduction plant in Food & Grain Hub.",
        current_status="ACTIVE_DEMAND"
    ),
    Facility(
        id="CONS-03",
        name="Company H",
        facility_type=FacilityType.CONSUMER,
        sector=IndustrySector.DISTRICT_HEATING,
        latitude=18.7550,
        longitude=73.7850,
        zone_id="zone-tech-corridor",
        temperature_c=78.0,
        mass_flow_rate_kg_s=75.0,
        fluid_medium=FluidMedium.WATER,
        thermal_capacity_mwt=19.2,
        operating_hours_year=8760,
        description="Regional smart city municipal hydronic loop supplying commercial parks and hospital complexes.",
        current_status="ACTIVE_DEMAND"
    ),
    Facility(
        id="CONS-04",
        name="Company I",
        facility_type=FacilityType.CONSUMER,
        sector=IndustrySector.CHEMICAL_PROCESSING,
        latitude=18.7600,
        longitude=73.7250,
        zone_id="zone-chemical-corridor",
        temperature_c=130.0,
        mass_flow_rate_kg_s=55.0,
        fluid_medium=FluidMedium.PRESSURIZED_WATER,
        thermal_capacity_mwt=17.5,
        operating_hours_year=8000,
        description="Feedstock pre-heater requiring steady high-enthalpy thermal energy before catalytic synthesis in Chemical Belt.",
        current_status="ACTIVE_DEMAND"
    ),
    Facility(
        id="CONS-05",
        name="Company J",
        facility_type=FacilityType.CONSUMER,
        sector=IndustrySector.PAPER_PULP,
        latitude=18.7000,
        longitude=73.8150,
        zone_id="zone-manufacturing-corridor",
        temperature_c=115.0,
        mass_flow_rate_kg_s=40.0,
        fluid_medium=FluidMedium.PRESSURIZED_WATER,
        thermal_capacity_mwt=9.8,
        operating_hours_year=8100,
        description="High-velocity cylinder paper drying tunnel requiring medium-grade thermal energy in Packaging Sector.",
        current_status="ACTIVE_DEMAND"
    ),
]

class ThermalRoutingEngine:
    """
    Advanced thermodynamic optimizer for district-scale waste heat networks.
    Computes rigorous physical properties, EN 253 pipe losses, hydraulic drops, and financial returns.
    """
    def __init__(self, facilities: Optional[List[Facility]] = None):
        self.facilities = list(facilities or DEFAULT_FACILITIES)
        self.producers = [f for f in self.facilities if f.facility_type == FacilityType.PRODUCER]
        self.consumers = [f for f in self.facilities if f.facility_type == FacilityType.CONSUMER]

    def add_facility(self, facility: Facility):
        """Adds a new company facility to the thermodynamic routing engine and updates topology."""
        self.facilities.append(facility)
        if facility.facility_type == FacilityType.PRODUCER:
            self.producers.append(facility)
        else:
            self.consumers.append(facility)

    @staticmethod
    def calculate_distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Haversine distance with 1.25x civil routing easement factor for buried pipeline routes."""
        R = 6371.0 # Earth radius in km
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        a = (math.sin(dlat / 2)**2 +
             math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2)**2)
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        direct_dist = R * c
        # 1.25 civil underground routing tortuosity factor
        return round(direct_dist * 1.25, 2)

    @staticmethod
    def select_pipe_diameter(mass_flow_kg_s: float, density: float = 980.0, target_velocity: float = 1.8) -> int:
        """Calculates internal diameter in mm and snaps to commercial nominal standards (EN 253)."""
        volumetric_flow = mass_flow_kg_s / density
        ideal_d_m = math.sqrt((4.0 * volumetric_flow) / (math.pi * target_velocity))
        ideal_d_mm = ideal_d_m * 1000.0

        standard_sizes = [80, 100, 125, 150, 200, 250, 300, 350, 400, 500]
        for size in standard_sizes:
            if size >= ideal_d_mm:
                return size
        return 500

    def evaluate_pairing(self, producer: Facility, consumer: Facility,
                         override_producer_temp: Optional[float] = None) -> Optional[RoutingMatch]:
        """
        Evaluates thermodynamic, hydraulic, and economic viability of a pipeline connection
        between an industrial heat producer and consumer.
        """
        source_temp = override_producer_temp if override_producer_temp is not None else producer.temperature_c
        req_temp = consumer.temperature_c

        # 1. Distance Calculation
        distance_km = self.calculate_distance_km(
            producer.latitude, producer.longitude,
            consumer.latitude, consumer.longitude
        )
        distance_m = distance_km * 1000.0

        # Max feasible direct piping distance threshold for district waste heat
        if distance_km > 12.0:
            return None

        # 2. Hydraulic Sizing & Flow
        design_mass_flow = min(producer.mass_flow_rate_kg_s, consumer.mass_flow_rate_kg_s * 1.1)
        pipe_dn_mm = self.select_pipe_diameter(design_mass_flow)
        pipe_d_m = pipe_dn_mm / 1000.0
        casing_d_m = pipe_d_m * settings.CASING_OUTER_DIAMETER_RATIO
        
        cross_section_area = math.pi * (pipe_d_m / 2.0)**2
        velocity = design_mass_flow / (980.0 * cross_section_area)

        # 3. Thermodynamic Heat Loss via EN 253 standard
        # Thermal resistance of Polyurethane insulation: R_ins = ln(D_casing / D_pipe) / (2 * pi * k_ins)
        r_ins = math.log(casing_d_m / pipe_d_m) / (2.0 * math.pi * settings.INSULATION_CONDUCTIVITY_PUR)
        # Soil thermal resistance: R_soil = ln(4 * H / D_casing) / (2 * pi * k_soil)
        r_soil = math.log(4.0 * 1.2 / casing_d_m) / (2.0 * math.pi * 1.2)
        total_r = r_ins + r_soil

        mean_fluid_temp = (source_temp + req_temp) / 2.0
        q_loss_per_meter = (mean_fluid_temp - settings.AMBIENT_GROUND_TEMP_C) / total_r # W/m
        total_loss_w = q_loss_per_meter * distance_m
        total_loss_mwt = round(total_loss_w / 1e6, 3)

        # Temperature drop over pipeline
        cp = 4.184 # kJ/(kg·K)
        temp_drop_c = total_loss_w / (design_mass_flow * cp * 1000.0)
        delivered_temp_c = round(source_temp - temp_drop_c, 1)

        # Pinch Temperature Margin
        pinch_margin_c = round(delivered_temp_c - req_temp, 1)

        # If delivered temperature is colder than required demand temperature,
        # it requires heat pump COP boosting or is physically unfeasible
        if delivered_temp_c < (req_temp - 2.0):
            return None

        # Thermal Efficiency of transmission
        heat_transferred_mwt = min(producer.thermal_capacity_mwt, consumer.thermal_capacity_mwt)
        efficiency_pct = round(max(50.0, min(98.5, (1.0 - (total_loss_mwt / max(0.5, heat_transferred_mwt))) * 100.0)), 1)

        # 4. CAPEX & OPEX Economics
        # Excavation, trenching, twin-pipe installation cost scales with diameter
        cost_per_meter = settings.EXCAVATION_COST_PER_METER * (1.0 + (pipe_dn_mm / 300.0) * 0.45)
        piping_capex = distance_m * cost_per_meter
        heat_exchanger_capex = 180000.0 + (heat_transferred_mwt * 35000.0) # Plate Heat Exchanger (HEX)
        pumping_station_capex = 120000.0 + (distance_km * 18000.0)
        total_capex = round(piping_capex + heat_exchanger_capex + pumping_station_capex, 0)

        # Hydraulic Head Loss & Pumping OPEX
        # Darcy-Weisbach head loss
        friction_factor = 0.02
        delta_p_pa = friction_factor * (distance_m / pipe_d_m) * (980.0 * velocity**2 / 2.0)
        volumetric_flow_m3_s = design_mass_flow / 980.0
        pump_power_kw = (volumetric_flow_m3_s * delta_p_pa) / (0.75 * 1000.0) # 75% pump efficiency
        
        operating_hours = min(producer.operating_hours_year, consumer.operating_hours_year)
        annual_pump_kwh = pump_power_kw * operating_hours
        annual_pump_electricity_cost = annual_pump_kwh * settings.GRID_ELECTRICITY_PRICE_PER_KWH
        annual_maintenance_cost = total_capex * 0.012
        total_opex_yr = round(annual_pump_electricity_cost + annual_maintenance_cost, 0)

        # 5. Energy Delivered, Savings & Emissions
        annual_energy_mwh = round(heat_transferred_mwt * operating_hours * (efficiency_pct / 100.0), 1)
        # Fuel displaced: natural gas at 82% baseline boiler efficiency
        displaced_fuel_mwh = annual_energy_mwh / settings.BOILER_EFFICIENCY_BASELINE
        fuel_cost_savings_yr = displaced_fuel_mwh * settings.NATURAL_GAS_DISPLACEMENT_COST_PER_MWH
        
        # Carbon savings: 202 kg CO2 / MWh natural gas
        co2_avoided_tonnes_yr = round((displaced_fuel_mwh * settings.GAS_EMISSION_FACTOR_KG_PER_MWH) / 1000.0, 1)
        carbon_credit_revenue_yr = co2_avoided_tonnes_yr * settings.CARBON_TAX_CREDIT_PER_TONNE

        gross_annual_benefit_yr = fuel_cost_savings_yr + carbon_credit_revenue_yr
        net_financial_savings_yr = round(gross_annual_benefit_yr - total_opex_yr, 0)

        # Payback period
        payback_years = round(total_capex / max(1.0, net_financial_savings_yr), 2)
        
        # Levelized Cost of Heat (LCOH, $/MWh_t) over 25-year asset life, 6% discount rate (CRF = 0.0782)
        crf = 0.0782
        lcoh_usd_mwh = round(((total_capex * crf) + total_opex_yr) / max(1.0, annual_energy_mwh), 2)

        # Feasibility Composite Score (0 to 100)
        # Favors: high efficiency, low payback period, positive pinch margin, low distance
        score = 100.0
        score -= min(35.0, payback_years * 3.5)
        score -= min(25.0, distance_km * 2.5)
        score += min(15.0, (pinch_margin_c - 5.0) * 0.8)
        score = round(max(30.0, min(99.0, score)), 1)

        match_id = f"MATCH-{producer.id}-{consumer.id}"

        # Generate smooth polyline coordinates for map visualization
        pipeline_coords = [
            [producer.latitude, producer.longitude],
            # Midpoint with slight organic routing offset
            [
                (producer.latitude + consumer.latitude) / 2.0 + 0.0015,
                (producer.longitude + consumer.longitude) / 2.0 - 0.0015
            ],
            [consumer.latitude, consumer.longitude]
        ]

        inr_rate = settings.USD_TO_INR_RATE
        capex_inr = round(total_capex * inr_rate, 0)
        opex_inr_yr = round(total_opex_yr * inr_rate, 0)
        net_financial_savings_inr_yr = round(net_financial_savings_yr * inr_rate, 0)
        lcoh_inr_mwh = round(lcoh_usd_mwh * inr_rate, 2)

        return RoutingMatch(
            match_id=match_id,
            producer_id=producer.id,
            producer_name=producer.name,
            producer_sector=producer.sector.value,
            producer_temp_c=round(source_temp, 1),
            consumer_id=consumer.id,
            consumer_name=consumer.name,
            consumer_sector=consumer.sector.value,
            consumer_required_temp_c=req_temp,
            distance_km=distance_km,
            pipe_diameter_mm=pipe_dn_mm,
            fluid_velocity_m_s=round(velocity, 2),
            heat_loss_mwt=total_loss_mwt,
            delivered_temp_c=delivered_temp_c,
            pinch_temperature_margin_c=pinch_margin_c,
            thermal_efficiency_pct=efficiency_pct,
            capex_usd=total_capex,
            capex_inr=capex_inr,
            opex_usd_yr=total_opex_yr,
            opex_inr_yr=opex_inr_yr,
            annual_energy_mwh=annual_energy_mwh,
            financial_savings_usd_yr=net_financial_savings_yr,
            financial_savings_inr_yr=net_financial_savings_inr_yr,
            co2_avoided_tonnes_yr=co2_avoided_tonnes_yr,
            payback_years=payback_years,
            lcoh_usd_mwh=lcoh_usd_mwh,
            lcoh_inr_mwh=lcoh_inr_mwh,
            feasibility_score=score,
            pipeline_coordinates=pipeline_coords
        )

    def calculate_all_matches(self, live_telemetry: Optional[Dict[str, float]] = None) -> List[RoutingMatch]:
        """Calculates and ranks all potential producer-consumer routing matches."""
        matches: List[RoutingMatch] = []
        for prod in self.producers:
            override_temp = live_telemetry.get(prod.id) if live_telemetry else None
            for cons in self.consumers:
                match = self.evaluate_pairing(prod, cons, override_producer_temp=override_temp)
                if match:
                    matches.append(match)

        # Sort by feasibility score descending
        matches.sort(key=lambda m: m.feasibility_score, reverse=True)
        return matches

# Singleton instance
thermal_engine = ThermalRoutingEngine()
