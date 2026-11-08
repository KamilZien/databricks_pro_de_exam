"""
Module 6 — Subpoint 1.8: Structured Streaming vs. Lakeflow Declarative Pipelines.

Demonstrates the architectural decision boundary:
  1. Lakeflow Declarative Pipeline:
     - Managed checkpoints (no manual paths)
     - Declarative data quality expectations (@dlt.expect_or_drop)
     - Unified Streaming Tables + Materialized Views
  2. Spark Structured Streaming (Imperative):
     - Manual checkpoint management
     - Custom micro-batch foreachBatch handler
     - Routing alerts to external systems / custom sinks
"""

from typing import Any


# ===========================================================================
# 1. Declarative Lakeflow Pipeline Abstraction
# ===========================================================================
def define_declarative_pipeline_spec() -> dict[str, Any]:
    """
    Returns the declarative pipeline specification.
    Key properties:
      - NO manual checkpointLocation
      - Data quality expectations defined declaratively
      - Automatically managed DAG
    """
    return {
        "framework": "Lakeflow Declarative Pipelines",
        "paradigm": "declarative",
        "managed_checkpoints": True,
        "expectations": {
            "valid_amount": "amount > 0",
            "action_on_violation": "DROP",
        },
        "supported_targets": ["STREAMING TABLE", "MATERIALIZED VIEW", "AUTO CDC"],
    }


# ===========================================================================
# 2. Imperative Spark Structured Streaming Abstraction
# ===========================================================================
class ImperativeAlertSink:
    """
    Simulates writing anomalous records to an external custom sink
    (such as a Kafka topic, Redis cache, or PagerDuty webhook) via foreachBatch.
    """

    def __init__(self):
        self.emitted_alerts: list[dict[str, Any]] = []

    def process_micro_batch(self, batch_id: int, records: list[dict[str, Any]]) -> int:
        """
        Micro-batch processor simulating a custom foreachBatch writer.
        Requires manual transaction tracking and custom error recovery.
        """
        alerts_sent = 0
        for row in records:
            if row.get("amount", 0) > 10000:  # High-value fraud alert
                self.emitted_alerts.append(
                    {
                        "batch_id": batch_id,
                        "user_id": row.get("user_id"),
                        "amount": row.get("amount"),
                        "alert_type": "HIGH_VALUE_TRANSACTION",
                    }
                )
                alerts_sent += 1
        return alerts_sent


def build_imperative_stream_config(catalog: str, checkpoint_volume: str) -> dict[str, Any]:
    """
    Constructs an imperative Structured Streaming configuration.
    Demonstrates that imperative streaming REQUIRES:
      - Manual checkpointLocation specification
      - Explicit trigger policy (.trigger(availableNow=True))
      - Custom error/listener handling
    """
    return {
        "framework": "Spark Structured Streaming",
        "paradigm": "imperative",
        "manual_checkpoints": True,
        "checkpoint_location": f"{checkpoint_volume}/_checkpoints/fraud_alerts",
        "trigger": "availableNow",
        "custom_sink_enabled": True,
    }


if __name__ == "__main__":
    dlt_spec = define_declarative_pipeline_spec()
    print(f"[*] Declarative Lakeflow Spec: Managed Checkpoints = {dlt_spec['managed_checkpoints']}")

    sink = ImperativeAlertSink()
    mock_batch = [
        {"user_id": "U-1", "amount": 500},
        {"user_id": "U-2", "amount": 15000},
        {"user_id": "U-3", "amount": 25000},
    ]
    emitted = sink.process_micro_batch(batch_id=1, records=mock_batch)
    print(f"[+] Imperative foreachBatch Emitted {emitted} external alerts.")
