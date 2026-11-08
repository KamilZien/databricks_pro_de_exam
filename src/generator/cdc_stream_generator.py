"""
Module 5 — CDC Data Generator: Customer Account Lifecycle Stream.

Generates synthetic Change Data Capture (CDC) events representing real-world database changes:
  - INSERT: New customer registers
  - UPDATE: Customer changes tier, country, or contact email
  - DELETE: Customer deletes account / GDPR right-to-be-forgotten
  - OUT-OF-ORDER: Network-delayed events simulating real distributed message lag

These events feed directly into Subpoint 1.7's AUTO CDC pipeline (SCD Type 1 & Type 2).
"""

import json
from datetime import datetime, timedelta
from typing import Any


def generate_customer_cdc_events() -> list[dict[str, Any]]:
    """
    Generates a calibrated batch of CDC events designed to test:
      1. Basic INSERTs
      2. Sequential UPDATEs
      3. Out-of-order UPDATEs (older timestamp arriving AFTER newer timestamp)
      4. DELETE operations
    """
    base_time = datetime(2026, 10, 25, 10, 0, 0)

    events = [
        # Customer 101: Clean Lifecycle (INSERT -> UPDATE -> UPDATE)
        {
            "customer_id": "CUST-101",
            "name": "Acme Corp",
            "tier": "STANDARD",
            "country": "DE",
            "operation": "INSERT",
            "event_timestamp": (base_time).isoformat(),
        },
        {
            "customer_id": "CUST-101",
            "name": "Acme Corp",
            "tier": "ENTERPRISE",
            "country": "DE",
            "operation": "UPDATE",
            "event_timestamp": (base_time + timedelta(hours=2)).isoformat(),
        },
        {
            "customer_id": "CUST-101",
            "name": "Acme Global Corp",
            "tier": "ENTERPRISE",
            "country": "CH",
            "operation": "UPDATE",
            "event_timestamp": (base_time + timedelta(hours=5)).isoformat(),
        },
        # Customer 102: Delete Lifecycle (INSERT -> UPDATE -> DELETE)
        {
            "customer_id": "CUST-102",
            "name": "Beta Labs",
            "tier": "TRIAL",
            "country": "US",
            "operation": "INSERT",
            "event_timestamp": (base_time).isoformat(),
        },
        {
            "customer_id": "CUST-102",
            "name": "Beta Labs",
            "tier": "STANDARD",
            "country": "US",
            "operation": "UPDATE",
            "event_timestamp": (base_time + timedelta(hours=1)).isoformat(),
        },
        {
            "customer_id": "CUST-102",
            "name": "Beta Labs",
            "tier": "STANDARD",
            "country": "US",
            "operation": "DELETE",
            "event_timestamp": (base_time + timedelta(hours=4)).isoformat(),
        },
        # Customer 103: OUT-OF-ORDER TRAP (Crucial for testing SEQUENCE BY)
        # Event A: Registered at 10:00 (TIER: TRIAL)
        # Event B: Upgraded at 12:00 (TIER: VIP) — arrives in stream FIRST
        # Event C: Changed address at 11:00 (TIER: TRIAL, Country: FR) — arrives LATE
        {
            "customer_id": "CUST-103",
            "name": "Gamma AI",
            "tier": "TRIAL",
            "country": "UK",
            "operation": "INSERT",
            "event_timestamp": (base_time).isoformat(),
        },
        # Arrives second: The newer update (12:00)
        {
            "customer_id": "CUST-103",
            "name": "Gamma AI",
            "tier": "VIP",
            "country": "UK",
            "operation": "UPDATE",
            "event_timestamp": (base_time + timedelta(hours=2)).isoformat(),
        },
        # Arrives third in message order, BUT its source timestamp is 11:00 (older than 12:00!)
        # With SEQUENCE BY event_timestamp, this MUST NOT overwrite tier='VIP'!
        {
            "customer_id": "CUST-103",
            "name": "Gamma AI",
            "tier": "TRIAL",
            "country": "FR",
            "operation": "UPDATE",
            "event_timestamp": (base_time + timedelta(hours=1)).isoformat(),
        },
    ]

    return events


if __name__ == "__main__":
    cdc_records = generate_customer_cdc_events()
    print(f"[*] Generated {len(cdc_records)} calibrated CDC events.")
    print(json.dumps(cdc_records[:2], indent=2))
