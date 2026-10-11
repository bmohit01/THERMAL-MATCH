# ThermalMatch — System Architecture Specification
**Enterprise B2B Industrial Heat Waste Exchange Platform**  
*Document Version: 2.4.0 | Target Cloud: AWS & Google Antigravity | Currency: INR (₹)*

---

## 1. Executive Summary & Vision

**ThermalMatch** is an enterprise-grade, high-throughput B2B Industrial Heat Waste Exchange platform that captures, calculates, and monetizes industrial thermal energy that would otherwise be rejected into the atmosphere. The platform bridges the thermodynamic gap between:
- **Thermal Producers** (liquid-cooled hyperscale data centers, iron and steel foundries, chemical plants, and flat glass furnaces producing 80°C to 260°C exhaust streams).
- **Thermal Consumers** (agricultural desiccation silos, district heating networks, paper pulp drying mills, and chemical pre-heating reactors demanding 60°C to 130°C thermal energy).

The system pairs a **minimalist, ultra-clean web interface** with a **thermodynamic routing engine**, **Amazon Kinesis SCADA streaming pipelines**, **Amazon QuickSight signed ESG embed ledgers**, and **Google Gemini AI advisory agents** governed by the **Google Antigravity SDK**.

```mermaid
graph TB
    subgraph IndustrialSources["Industrial Facilities (Regional Industrial Corridor)"]
        P1["Company A Cloud Center (82°C)"]
        P2["Company B Smelting Foundry (260°C)"]
        P3["Company D PetroChemical Cracker (145°C)"]
        P4["Company C Precision Glass Works (195°C)"]
        P5["Company E Distillation Works (94°C)"]
    end

    subgraph StreamingIngest["Real-Time Streaming Tier (Amazon Kinesis)"]
        K1["Shard-0001 (Thermal Ingest)"]
        K2["Shard-0002 (Pressure & Flow)"]
        K3["Shard-0003 (Enthalpy Shard)"]
        K4["Shard-0004 (Anomaly Stream)"]
    end

    subgraph CoreEngine["FastAPI Core & Thermodynamic Engine"]
        API["FastAPI REST & ASGI Controller"]
        TE["Thermodynamic Routing Engine (EN 253)"]
        PA["Pinch Analysis (ΔT_pinch ≥ 5°C)"]
        HY["Hydraulic Head Loss (Darcy-Weisbach)"]
        EC["Financial & LCOH Model (INR ₹)"]
    end

    subgraph IntelligenceTier["AI Advisory Tier (Google Gemini & Antigravity)"]
        AG["Antigravity Agent Orchestrator"]
        GM["Gemini Pro Industrial Model (API: AQ.Ab8...)"]
        CI["Context Injection (Kinesis + Routing Matrix)"]
    end

    subgraph AnalyticsTier["Executive Governance & Analytics"]
        QS["Amazon QuickSight Service"]
        STS["AWS STS AssumeRole (RLS Signed URLs)"]
        HLDC["EN 253 Heat Load Duration Ledger"]
    end

    subgraph FrontendApp["Minimalist Frontend (AWS Amplify Hosting)"]
        DASH["Executive Dashboard (INR ₹)"]
        MAP["Dynamic Vector Map Canvas"]
        THEME["Dual Theme Engine (Cyan/Blue & Thermal Red)"]
        CHAT["Gemini Assistant Dialog"]
    end

    IndustrialSources -->|SCADA Modbus/TCP| StreamingIngest
    StreamingIngest --> CoreEngine
    CoreEngine --> API
    API --> FrontendApp
    CoreEngine --> CI --> GM --> AG --> API
    CoreEngine --> QS --> STS --> FrontendApp
```

---

## 2. High-Level Architectural Layers

The system is decomposed into 5 decoupled, fault-tolerant tiers:

| Architectural Tier | Primary Technology | Purpose & Responsibility |
| :--- | :--- | :--- |
| **Tier 1: Frontend Presentation** | Vanilla JS, Modern CSS, HTML5, SVG Canvas | Zero-dependency, sub-second load times, responsive vector map, theme switching, AWS Amplify hosting. |
| **Tier 2: API & Routing Engine** | FastAPI, Python 3.11+, Pydantic v2 | High-concurrency async REST layer, EN 253 pipe heat loss calculations, Darcy-Weisbach hydraulics. |
| **Tier 3: Streaming Telemetry** | Amazon Kinesis Data Streams | Ingestion of high-frequency SCADA/PLC telemetry (temperature, mass flow, pressure) across multiple shards. |
| **Tier 4: Generative AI Advisory** | Google Gemini, Google Antigravity SDK | Autonomous agent reasoning, multi-metric prompt synthesis, thermodynamic recommendations in INR (₹). |
| **Tier 5: Secure ESG Analytics** | Amazon QuickSight, AWS STS, HMAC-SHA256 | Row-Level Security (RLS) scoped embedded dashboards, 8,760-hour load duration curves, cumulative NPV waterfall. |

---

## 3. Tier 1: Frontend Architecture & Design System

The frontend adheres to an ultra-clean, minimalist design philosophy that avoids visual clutter while offering deep operational capability:

```mermaid
graph LR
    subgraph UIStructure["Frontend Component Hierarchy"]
        Rail["Navigation Rail (Left Rail Only)"]
        Header["Non-Duplicated Header (Breadcrumbs + Live Pulse)"]
        Bento["Executive Bento Grid (4 Top KPIs in ₹)"]
        ViewSwitch["View Switcher (Directory Table ⟷ Vector Map)"]
        Modals["Flyout Modals (Gemini Chat + QuickSight ESG)"]
    end
    Rail --> Header --> Bento --> ViewSwitch --> Modals
```

### 3.1 Design Principles & Tokens
1. **Consolidated Navigation Rail**: All navigation lives exclusively on the left vertical rail (`Overview`, `Pipelines`, `Vector Map`, `Streams`, `ESG Audit`, `AI Advisor`, `Settings`). The top header is completely deduplicated and contains only contextual breadcrumbs and a live SCADA heartbeat badge.
2. **Dynamic Dual Theme Engine**:
   - **Blue-Based Theme (Default)**: High-contrast Cyan (`#06b6d4`), Cyan Hover (`#0891b2`), Pale Cyan (`#ecfeff`), and Emerald Green (`#00a86b`).
   - **Red-Based Theme (Thermal)**: Thermal Crimson (`#ef4444`), Flame Red (`#dc2626`), Pale Blush (`#fef2f2`), and Coral Red (`#ea3a4b`).
   - Persisted across user sessions via `localStorage.getItem('thermalmatch_theme')`.
3. **Typography**: Google Fonts `Outfit` (for executive readability and branding) paired with `JetBrains Mono` (for engineering telemetry, temperatures, and rupee currency metrics).

### 3.2 Dynamic Vector Map Canvas
- Custom SVG projection mapping geographic coordinates of the **Regional Industrial Corridor** (`18.670°N – 18.785°N`, `73.700°E – 73.850°E`) directly onto a `1200x600` canvas.
- Producers rendered as hot thermal emitters (orange/coral); consumers rendered as demand sinks (cyan/teal).
- Pipeline connections feature dynamically animated dashed flows with thickness proportional to thermal flux (MWt).

---

## 4. Tier 2: Thermodynamic Routing & Economic Optimizer

The core engine implements rigorous physical models conforming to international district energy standards (**EN 253**, **EN 13941**, and **VDI 2067**).

```mermaid
flowchart TD
    Start["Pairing Request (Producer i ➔ Consumer j)"] --> Dist["Haversine Geodetic Distance * 1.25 Easement Factor"]
    Dist --> Pinch{"Pinch Check: T_supply ≥ T_demand + ΔT_pinch (5°C)?"}
    Pinch -- No --> Reject["Infeasible Match (Pinch Violation)"]
    Pinch -- Yes --> PipeLoss["EN 253 Pre-Insulated Pipe Heat Loss Calculation"]
    PipeLoss --> Hydraulic["Darcy-Weisbach Hydraulic Friction & Pumping Work"]
    Hydraulic --> Econ["Economic Valuation: CAPEX, LCOH, Rupee Savings"]
    Econ --> Rank["Rank by Thermal Efficiency & Payback Period"]
```

### 4.1 Heat Loss Formulation (EN 253 Standard)
The thermal loss rate $q$ (Watts per meter of pipe) for buried pre-insulated twin pipes:

$$q = \frac{T_{supply} - T_{ground}}{R_{pipe} + R_{insulation} + R_{soil}}$$

Where:
- $R_{insulation} = \frac{\ln(D_{outer} / D_{inner})}{2 \pi \lambda_{PUR}}$
- $\lambda_{PUR} = 0.026 \text{ W/(m}\cdot\text{K)}$ (Cyclopentane-blown bonded polyurethane foam)
- Fluid temperature drop along pipeline of length $L$:

$$\Delta T_{fluid} = \frac{q \cdot L}{\dot{m} \cdot c_p}$$

Where $\dot{m}$ is mass flow rate (kg/s) and $c_p$ is specific heat capacity of water/steam ($4.184 \text{ kJ/(kg}\cdot\text{K)}$).

### 4.2 Hydraulic Model (Darcy-Weisbach)
Head loss $h_f$ and required electrical pumping power $P_{pump}$:

$$h_f = f \cdot \frac{L}{D} \cdot \frac{v^2}{2g}$$

$$P_{pump} = \frac{\rho \cdot g \cdot Q \cdot h_f}{\eta_{pump}}$$

Friction factor $f$ computed iteratively via the Swamee-Jain approximation of the Colebrook-White equation for steel pipe roughness $\epsilon = 0.045\text{ mm}$.

### 4.3 Pinch Analysis
A match is flagged as thermodynamically valid if and only if:

$$T_{delivered} \ge T_{required} + \Delta T_{pinch} \quad (\Delta T_{pinch} = 5.0^\circ\text{C})$$

### 4.4 Financial Economics in Indian Rupees (₹)
- **Pipe CAPEX**: Conforms to Indian urban trenching standards ($C_{meter} = ₹12,000 \text{ to } ₹28,000\text{ / meter}$ depending on nominal diameter DN100 – DN300).
- **Displaced Fuel Savings**: Displacing natural gas combustion ($₹1,850\text{ / MMBtu}$) and industrial fuel oil.
- **Payback Period**:

$$\text{Payback (Years)} = \frac{\text{Total Installed CAPEX (₹)}}{\text{Annual Net Operational Savings (₹)}}$$

---

## 5. Tier 3: Real-Time Telemetry Streaming (Amazon Kinesis)

```mermaid
sequenceDiagram
    autonumber
    participant SCADA as Factory SCADA / PLC
    participant Ingest as /api/kinesis/ingest
    participant Kinesis as Amazon Kinesis Shards
    participant Anomaly as Anomaly Detector
    participant State as Grid Telemetry State
    participant Frontend as Browser Client (SSE/Poll)

    SCADA->>Ingest: HTTP POST Telemetry (Temp, Flow, Press)
    Ingest->>Kinesis: PutRecord (PartitionKey=FacilityId)
    Kinesis->>Anomaly: Stream Processor (Window = 60s)
    Anomaly-->>State: Emit Alert if dT/dt > 15°C/min
    Kinesis->>State: Update Live Shard State
    Frontend->>State: GET /api/kinesis/telemetry/live (Every 3s)
    State-->>Frontend: Real-Time Array (Producers & Shards)
```

- **Partition Key Strategy**: Partitioned by `facility_id` ensuring strict FIFO ordering per industrial exhaust source.
- **Throughput Capability**: Supports up to 1,000 records/sec per shard with sub-second propagation into the dispatch matrix.
- **Anomaly Detection**: Flags sudden temperature collapse, thermal shock (>15°C/minute gradient), and sensor drift in real time.

---

## 6. Tier 4: Generative AI Advisory Layer (Google Gemini)

```mermaid
graph TD
    UserQuery["User Prompt: 'How can we utilize 80°C exhaust air?'"] --> Controller["/api/gemini/query Endpoint"]
    Controller --> ContextBuilder["Real-Time Context Aggregator"]
    
    subgraph ContextAssembly["Context Assembly Pipeline"]
        KTelemetry["1. Live Kinesis SCADA Temperatures"]
        Candidates["2. Ranked Thermodynamic Match Candidates"]
        DistGrid["3. District Ambient & Fuel Costs (₹ INR)"]
    end
    
    ContextAssembly --> PromptInject["Construct Structured Grounded Prompt"]
    PromptInject --> GeminiSDK["Google Antigravity Agent Runtime"]
    GeminiSDK --> ModelCall["Gemini Pro Model (Auth via GEMINI_API_KEY)"]
    ModelCall --> ParseOutput["Parse Thermodynamic Recommendations & Financial Breakdown"]
    ParseOutput --> Response["Deliver JSON Advisory + Formatted Markdown"]
```

### Context Injection Schema
Before sending prompts to Google Gemini, the backend automatically gathers:
1. **Live Shard Telemetry**: Real-time temperatures and flow rates from all 5 district producers.
2. **Thermal Routing Candidates**: The top 3 viable pipeline pairs, including distance, pinch margin, and LCOH.
3. **District Constraints**: Local natural gas baseline prices, ambient ground temperatures, and carbon credit offsets.

---

## 7. Tier 5: Amazon QuickSight Secure ESG Embedded Layer

```mermaid
sequenceDiagram
    autonumber
    participant Browser as Browser Client
    participant Backend as FastAPI QuickSight Service
    participant STS as AWS STS AssumeRole
    participant QS as Amazon QuickSight Engine

    Browser->>Backend: GET /api/quicksight/embed-url
    Backend->>STS: AssumeRole (RoleArn=arn:aws:iam::.../QS-Federated)
    STS-->>Backend: Temporary STS Credentials + Session Token
    Backend->>Backend: Sign Payload with HMAC SHA-256 & Inject RLS Tags
    Backend-->>Browser: Signed Embed URL (exp = 600 mins)
    Browser->>QS: Embed IFrame / Render Vector SVG Visuals
    QS-->>Browser: 8,760-Hour HLDC & 6-Year NPV Waterfall Ledger
```

- **Row-Level Security (RLS)**: Enforces tenant isolation so district managers only view facilities within their authorized operational corridor.
- **Executive Ledger Metrics**:
  - **Heat Load Duration Curve (HLDC)**: 8,760 hours of thermal demand mapped against waste heat availability, demonstrating **100% fossil boiler displacement**.
  - **6-Year Cumulative NPV Waterfall**: Year 0 CAPEX outlay (-₹35.70 Cr) through Year 5 (+₹26.68 Cr net surplus), reaching break-even at **Year 2.87**.

---

## 8. Deployment Architecture & Infrastructure Topology

```mermaid
graph TB
    subgraph ClientDevices["Client Access Tier"]
        Desktop["Corporate Desktop (Chrome / Edge)"]
        Mobile["Industrial Tablet / SCADA HMI"]
    end

    subgraph AWSCloud["Amazon Web Services Infrastructure"]
        CF["Amazon CloudFront CDN (Global Edge)"]
        Amplify["AWS Amplify Hosting (frontend/)"]
        
        subgraph BackendVPC["Virtual Private Cloud (VPC)"]
            ALB["Application Load Balancer"]
            Fargate["AWS Fargate (FastAPI Container Task)"]
            KinesisAWS["Amazon Kinesis Data Streams (4 Shards)"]
            QuickSightAWS["Amazon QuickSight Enterprise Edition"]
        end
    end

    subgraph GoogleCloud["Google Cloud / Antigravity Services"]
        GeminiAPI["Google Gemini Generative AI (API ID: AQ.Ab8...)"]
        AntigravitySDK["Google Antigravity Agent Leasing Runtime"]
    end

    ClientDevices --> CF --> Amplify
    ClientDevices --> ALB --> Fargate
    Fargate --> KinesisAWS
    Fargate --> QuickSightAWS
    Fargate --> AntigravitySDK --> GeminiAPI
```

- **Frontend Hosting**: AWS Amplify Hosting (`amplify.yml`) providing atomic deployments, edge caching, and automated HTTPS certificate provisioning.
- **Backend Runtime**: Containerized FastAPI microservice running on AWS Fargate with auto-scaling based on telemetry ingest rates.
- **Network Security**: Strict Content Security Policy (CSP), HTTP Strict Transport Security (HSTS), and tenant-isolated session tokens.

---

## 9. Comprehensive System Data Flow

```mermaid
sequenceDiagram
    actor Director as Facility Director
    participant UI as Minimalist Dashboard
    participant API as FastAPI Backend
    participant Engine as Thermodynamic Engine
    participant Kinesis as Amazon Kinesis
    participant Gemini as Google Gemini AI

    Director->>UI: Select Filter: "Low-Grade (<75°C)"
    UI->>UI: Filter Table Rows & Highlight Map Pipelines
    UI->>API: GET /api/matches?limit=15
    API->>Kinesis: Read Latest Telemetry Records
    Kinesis-->>API: Active Temperatures (82°C – 260°C)
    API->>Engine: Re-calculate EN 253 Heat Loss & Payback
    Engine-->>API: 15 Feasible Pairings with Rupee Savings
    API-->>UI: JSON Routing Matches
    UI->>UI: Update Donut Flux Ring & Hourly Recovery Bars
    
    Director->>UI: Click "Consult Gemini AI Advisor"
    UI->>API: POST /api/gemini/query ("Optimize 80°C exhaust air")
    API->>Gemini: Dispatch Prompt with Injected Kinesis Telemetry
    Gemini-->>API: Thermodynamic Advisory + ₹ Financial Breakdown
    API-->>UI: Render Streamed Markdown Response
```

---

## 10. Repository File Structure

```
d:/New folder (2)/
├── amplify/                       # AWS Amplify backend configurations
├── amplify.yml                    # AWS Amplify CI/CD hosting pipeline & CSP headers
├── antigravity_agent.py           # Google Antigravity Agent leasing & SDK orchestrator
├── GEMINI.md                      # Architecture rules, Gemini API ID & project guidelines
├── ARCHITECTURE.md                # System Architecture Specification (This Document)
│
├── backend/                       # Python FastAPI Microservice Core
│   ├── config.py                  # Pydantic Settings, AWS credentials & API keys
│   ├── models.py                  # Domain Pydantic models (Facility, Telemetry, Matches)
│   ├── thermal_engine.py          # EN 253 Heat Loss, Darcy-Weisbach & Pinch Analysis
│   ├── kinesis_pipeline.py        # Amazon Kinesis Data Streams simulation & ingest
│   ├── quicksight_service.py      # AWS STS AssumeRole signed embed URL generator
│   ├── gemini_service.py          # Gemini AI integration with context injection
│   └── main.py                    # FastAPI ASGI application & REST endpoint routers
│
└── frontend/                      # Minimalist Client Presentation Tier
    ├── index.html                 # Single-Page Dashboard with SVG Map & QuickSight Modal
    ├── styles.css                 # Modular Design Tokens, Cyan/Blue & Thermal Red Themes
    ├── app.js                     # State controller, Vector Map projection & Theme Switcher
    └── aws-exports.js             # AWS Amplify client configurations
```

---

## 11. User Persona Authentication & Session Management

ThermalMatch incorporates a responsive corporate persona and authentication manager supporting both single-click operational role delegation and custom corporate SSO login credentials.

```mermaid
sequenceDiagram
    autonumber
    actor User as Plant Operator / Auditor
    participant Widget as Header Profile Widget (#user-profile-widget)
    participant Dropdown as User Dropdown Menu (#user-dropdown-menu)
    participant Modal as Auth & Persona Modal (#login-modal)
    participant Storage as LocalStorage (thermalmatch_user)
    participant UI as Dynamic Greeting & Banner

    User->>Widget: Click profile badge (Initials, Full Name, Scoped Role)
    Widget->>Dropdown: Toggle active menu
    alt Quick Switch Preset Role
        User->>Dropdown: Select preset (e.g. "Company A (Plant Manager)")
        Dropdown->>Storage: Persist session {name, role, email, initials}
        Storage->>UI: Update header avatar ("PM") & greeting ("Good evening, Plant Manager!")
        Dropdown->>User: Toast notification confirming role delegation
    else Custom User Authentication
        User->>Dropdown: Click "Login / Change User"
        Dropdown->>Modal: Open modal with pinned header & scrollable form
        User->>Modal: Enter Full Name, Role, Facility affiliation & Email
        Modal->>Storage: Compute initials and persist authenticated session
        Storage->>UI: Update header initials, role badge & time-of-day greeting
        Modal->>User: Toast notification ("Authenticated as [Name] · [Role]")
    else Sign Out / Guest Mode
        User->>Dropdown: Click "Sign Out / Guest"
        Dropdown->>Storage: Set Guest session {initials: "GU", role: "Read-Only Viewer"}
        Storage->>UI: Update header badge to "GU" and greeting to "Good evening, Guest User!"
    end
```

### Supported Personas:
1. **Facility Director** (`FD`): Regional Grid Admin (Regional Corridor Authority)
2. **Plant Manager** (`PM`): Company A (HyperScale Data Center)
3. **Smelting Lead** (`SL`): Company B (Heavy Metallurgical Foundry)
4. **Chemical Director** (`CD`): Company D (Petrochemicals Complex)
5. **Agro Operations** (`AO`): Company F (High-Tech Greenhouses)
6. **ESG & Carbon Auditor** (`EA`): District Sustainability & Carbon Credits Ledger
7. **Custom Corporate User**: Any arbitrary enterprise operator with custom entity and role affiliation.

---

*ThermalMatch Architecture Specification v2.5.0 — Certified for Enterprise Industrial Waste Heat Recovery.*
