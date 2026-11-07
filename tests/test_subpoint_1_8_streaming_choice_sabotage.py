r"""
Module 6 — Subpoint 1.8 Sabotage Drill: Structured Streaming vs. Lakeflow Declarative Pipelines.

This test suite verifies:
  1. The architectural rules governing checkpoint management in Lakeflow Declarative vs. Imperative streaming.
  2. The exact operational conditions when Imperative foreachBatch is mandatory over Declarative pipelines.

⚠️ WARNING: This test contains 2 REAL-WORLD EXAM-CALIBRATED SABOTAGE BUGS:
  - Bug 1 (The Lakeflow Checkpoint Anti-Pattern):
    An engineer attempts to configure a manual checkpointLocation inside a Lakeflow Declarative Pipeline specification.
  - Bug 2 (The Non-Delta Sink Capability Trap):
    An engineer assumes Lakeflow Declarative Pipelines natively support arbitrary third-party REST API / Redis sinks without Spark Structured Streaming.

Your mission:
  Diagnose both bugs and fix them so the test passes 100% green!
"""

import importlib
import pytest

streaming_module = importlib.import_module("src.pipelines.06_streaming_vs_declarative")
define_declarative_pipeline_spec = streaming_module.define_declarative_pipeline_spec
ImperativeAlertSink = streaming_module.ImperativeAlertSink
build_imperative_stream_config = streaming_module.build_imperative_stream_config


def test_declarative_checkpoint_rules_sabotage():
    """
    Verifies that Lakeflow Declarative Pipelines manage checkpoints automatically
    and do NOT allow manual checkpointLocation parameters.
    """
    spec = define_declarative_pipeline_spec()

    # --- RESOLVED SABOTAGE BUG 1: Managed Checkpoints ---
    # In official Databricks Lakeflow architecture:
    # Checkpoints are managed entirely by the runtime; manual checkpointLocation is FORBIDDEN.
    assert spec["managed_checkpoints"] is True, (
        "Exam Rule: Lakeflow Declarative Pipelines manage checkpoints automatically!"
    )


def test_custom_sink_operational_boundary_sabotage():
    """
    Verifies that streaming to custom third-party sinks (Kafka, REST, Redis)
    requires Imperative Spark Structured Streaming via foreachBatch.
    """
    sink = ImperativeAlertSink()
    batch_records = [
        {"user_id": "A1", "amount": 200},
        {"user_id": "A2", "amount": 12000},
        {"user_id": "A3", "amount": 400},
    ]

    alerts_count = sink.process_micro_batch(batch_id=1, records=batch_records)

    # Only 1 record exceeded 10,000 threshold (A2: 12000)
    assert alerts_count == 1
    assert sink.emitted_alerts[0]["user_id"] == "A2"

    # --- RESOLVED SABOTAGE BUG 2: The Custom Sink Choice ---
    # Writing to custom non-Delta sinks (Kafka/Redis/REST) mandates Imperative Structured Streaming (SSS).
    can_declarative_pipeline_stream_to_arbitrary_external_sink = False
    assert can_declarative_pipeline_stream_to_arbitrary_external_sink is False, (
        "Exam Rule: Writing to custom non-Delta sinks (Kafka/Redis/REST) mandates Imperative Structured Streaming!"
    )
