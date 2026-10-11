"""
ThermalMatch Antigravity Agent Orchestrator.
Demonstrates programmatic agent leasing, thermodynamic tool wiring,
and live stream reasoning within the Google Antigravity SDK.
"""

import asyncio
import os
import sys

# Configure environment with specified Gemini API ID
os.environ.setdefault("GEMINI_API_ID", os.getenv("GEMINI_API_KEY", ""))

async def run_thermal_agent(query: str):
    """Executes a thermal engineering query through the Antigravity Agent runtime."""
    print("=" * 70)
    print("THERMALMATCH AGENT ENGINE — GOOGLE ANTIGRAVITY INITIALIZATION")
    print(f"API ID Authenticated: {os.environ.get('GEMINI_API_ID')}")
    print(f"Query: {query}")
    print("=" * 70)
    
    # Try importing google.antigravity if available, else use backend orchestrator
    try:
        from google.antigravity import Agent, LocalAgentConfig, CapabilitiesConfig
        config = LocalAgentConfig(
            system_instructions=(
                "You are the ThermalMatch Senior Thermodynamic Dispatch Engineer. "
                "You evaluate industrial waste heat exchanges between producers and consumers "
                "using real-time Amazon Kinesis telemetry and EN 253 thermal pipeline models."
            ),
            capabilities=CapabilitiesConfig(),
        )
        async with Agent(config) as agent:
            response = await agent.chat(query)
            async for token in response:
                sys.stdout.write(token)
                sys.stdout.flush()
            print()
    except (ImportError, Exception) as e:
        # Fallback to ThermalMatch backend Gemini integration
        from backend.gemini_service import GeminiThermalAssistant
        assistant = GeminiThermalAssistant()
        result = await assistant.generate_advisory(query)
        print("\n[Gemini Agent Response]:")
        print(result["advisory_text"])
        print("\n[Injected Context Metrics]:")
        for key, val in result["context_metadata"].items():
            print(f" - {key}: {val}")

if __name__ == "__main__":
    test_query = "What is the most cost-effective way to utilize 80°C exhaust air in this district?"
    asyncio.run(run_thermal_agent(test_query))
