---
description: Industrial heat waste exchange rules, thermodynamic equations, and AWS Kinesis/QuickSight protocols.
globs: ["backend/**/*.py", "frontend/**/*.js"]
---

# ThermalMatch Domain & Thermodynamic Engineering Guidelines

## 1. Thermodynamic Calculations & Equations
All thermal exchange models must adhere to:
1. **Heat Exchanger Transfer Rate**:
   $$\dot{Q} = \dot{m} \cdot c_p \cdot (T_{in} - T_{out})$$
   - Water: $c_p \approx 4.184 \text{ kJ/kg}\cdot\text{K}$, $\rho \approx 980 \text{ kg/m}^3$
   - Flue Gas: $c_p \approx 1.05 \text{ kJ/kg}\cdot\text{K}$
   - Thermal Oil (e.g. Dowtherm A): $c_p \approx 2.15 \text{ kJ/kg}\cdot\text{K}$
   - Steam: Enthalpy change $\Delta h = h_{steam} - h_{condensate}$

2. **Pipeline Heat Loss (EN 253 Standards)**:
   $$q_{loss} = \frac{2 \pi k_{ins}}{\ln(D_o / D_i)} \cdot (T_{fluid} - T_{ground}) \cdot L$$
   - Insulation conductivity $k_{ins} = 0.024 \text{ W/m}\cdot\text{K}$ (PUR foam)
   - Temperature drop over length $L$:
     $$\Delta T_{loss} = \frac{q_{loss}}{\dot{m} \cdot c_p}$$

3. **Hydraulic Pumping Parasitic Power**:
   $$\Delta P = f \cdot \frac{L}{D} \cdot \frac{\rho v^2}{2}$$
   $$P_{pump} = \frac{\dot{V} \cdot \Delta P}{\eta_{pump}}$$

## 2. Amazon Kinesis Telemetry Pipeline
- Shards must partition keys based on `facility_id` to guarantee in-order delivery of temperature sequences.
- Aggregate sliding window averages over 60-second and 300-second intervals.
- Trigger anomaly flags if temperature drift $> 15^\circ\text{C}$ in $< 30$ seconds.

## 3. Gemini Generative AI Assistant Contract
- Use configured ID via `GEMINI_API_KEY` / `GEMINI_API_ID` in `.env`.
- System prompts must inject real-time district context (live temperatures, enthalpy, nearby consumer demand).
- Deliver responses with concise engineering clarity, including CAPEX, OPEX, LCOH, and carbon savings.
