---
name: thermal-routing
description: Calculates thermodynamic heat loss, pinch temperature feasibility, pipeline routing economics, and Levelized Cost of Heat (LCOH) for industrial heat waste exchanges.
---

# Thermal Routing & Match-Making Skill

## When to use this skill
Use this skill when evaluating industrial heat recovery opportunities between industrial heat producers (e.g. data centers, smelters, refineries) and consumers (e.g. greenhouses, district heating, paper dryers).

## Thermodynamic Validation Steps
1. **Pinch Analysis Check**:
   - Verify that Producer Supply Temp $T_s \ge T_d + \Delta T_{pinch}$, where $\Delta T_{pinch} = 5^\circ\text{C}$.
   - If $T_s < T_d$, recommend an industrial heat pump (HP) with Coefficient of Performance (COP):
     $$\text{COP}_{actual} = \eta_{carnot} \cdot \frac{T_d + 273.15}{(T_d - T_s)}$$

2. **EN 253 Heat Loss Estimation**:
   - Compute thermal resistance $R_{th}$:
     $$R_{th} = \frac{\ln(D_{casing} / D_{pipe})}{2 \pi k_{ins}}$$
   - Loss per meter $q' = \frac{T_{mean} - T_{ground}}{R_{th}}$
   - Total loss $Q_{loss} = q' \cdot L$

3. **Financial Payback & LCOH**:
   $$\text{CAPEX} = (\text{PipingCost} \times L) + \text{HeatExchangerCost} + \text{PumpingStationCost}$$
   $$\text{AnnualRevenue} = \dot{Q}_{delivered} \times \text{HoursPerYear} \times \text{HeatPrice}$$
   $$\text{PaybackPeriod} = \frac{\text{CAPEX}}{\text{AnnualRevenue} - \text{OPEX}}$$
   $$\text{CO}_2 \text{ Saved (t/yr)} = \frac{\dot{Q}_{delivered} \times \text{Hours} \times \text{EmissionsFactor}_{gas}}{\eta_{boiler}}$$
