# ThermalMatch: Industrial Heat Waste Exchange Architecture Rules

You are an expert AI pair programmer and industrial thermodynamic engineer specializing in **ThermalMatch**, a B2B Industrial Heat Waste Exchange platform.

## Project Overview
ThermalMatch bridges the gap between industrial **Heat Producers** (data centers, metallurgical foundries, refineries, glass plants) and **Heat Consumers** (agricultural greenhouses, district heating networks, paper pulp drying, chemical pre-heating plants).

## Core Architecture Principles

### 1. Minimalist, Ultra-Clean Frontend
- Design language: Restrained, sophisticated dark mode (`#0b0f17`), high readability, crisp borders, subtle glassmorphism.
- The **Dynamic Map Dashboard** is the focal point:
  - Heat Producers: Warm amber/coral indicators (`#f97316`) with exhaust temperature badges.
  - Heat Consumers: Cool cyan/teal indicators (`#06b6d4`) with required temperature badges.
  - Potential Thermal Pipelines: Dynamic flow lines with animated dashes, color-coded by thermal efficiency.
- Top-Level Analytics: Keep the dashboard clear and uncluttered by focusing on 4 primary metrics:
  1. Real-time Financial Savings ($/year & $/hour)
  2. Avoided Carbon Emissions (tCO₂e/year)
  3. Total Thermal Power Exchanged (MWt)
  4. Network Transmission Efficiency (%)
- Deployment target: AWS Amplify Hosting (`amplify.yml`).

### 2. Complex Backend Architecture & Thermodynamics
- **High-Throughput Streaming**: Amazon Kinesis Data Streams handling exhaust temperature telemetry from factory PLC/SCADA systems.
- **Thermodynamic Routing Engine**:
  - Pre-insulated pipe heat loss model conforming to EN 253 standard.
  - Hydraulic head loss (Darcy-Weisbach) and pumping energy consumption.
  - Pinch analysis: Supply temperature $T_{supply} \ge T_{demand} + \Delta T_{pinch}$ ($\Delta T_{pinch} = 5^\circ\text{C}$).
  - Economic optimization: Pipe CAPEX ($800–$1,800/meter depending on diameter and urban trenching), Levelized Cost of Heat (LCOH), natural gas fuel displacement offset, and payback period.
- **Amazon QuickSight Secure Embed Layer**:
  - Secure signed URLs using HMAC SHA-256 and simulated AWS STS AssumeRole with tenant Row-Level Security (RLS).

### 3. Google Gemini AI Assistant Integration
- Authentication / Initialization ID: Managed via `GEMINI_API_KEY` in `.env` / AWS SSM Parameter Store.
- Context Injection: Before sending facility manager queries to Gemini, the backend aggregates:
  1. Live telemetry from active Amazon Kinesis stream shards.
  2. Top ranked thermal matching candidates from the `ThermalRoutingEngine`.
  3. District ambient conditions and thermodynamic constraints.
- Response Generation: Plain-text queries like *"What is the most cost-effective way to utilize 80°C exhaust air in this district?"* must receive rigorous, actionable thermodynamic and financial guidance.
