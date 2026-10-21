"""
Pytest configuration for Databricks Serverless tests.

Uses DatabricksSession.builder.serverless(True) — no local Spark/Java needed.
The session fixture is scoped to the module to avoid reconnecting per test.

Connection is configured via:
  - .databricks-connect.json (serverless: true)
  - .databrickscfg profile EXAM_PREP
  - Environment variables DATABRICKS_HOST / DATABRICKS_TOKEN

NEVER use:
  - .cache() / .persist()  → Spark Connect throws UnsupportedOperationException
  - .rdd / spark.sparkContext → not available on Spark Connect
  - Trigger.ProcessingTime → use .trigger(availableNow=True) for Serverless
"""

import os

import pytest


@pytest.fixture(scope="session", autouse=True)
def _set_databricks_env():
    """
    Load .env file variables into os.environ if not already set.
    This ensures Databricks connection works without manual export.
    """
    env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, _, value = line.partition("=")
                    os.environ.setdefault(key.strip(), value.strip())


@pytest.fixture(scope="session")
def spark():
    """
    Create a Databricks Serverless Spark session via Spark Connect.

    Connects to Databricks Free Edition Serverless compute.
    Uses EXAM_PREP profile from ~/.databrickscfg, falling back to
    DATABRICKS_HOST / DATABRICKS_TOKEN environment variables.
    """
    from databricks.connect import DatabricksSession

    session = DatabricksSession.builder.serverless(True).profile("EXAM_PREP").getOrCreate()
    yield session
    session.stop()


@pytest.fixture(scope="session")
def catalog_name():
    """Unity Catalog catalog name for the portfolio project."""
    return "portfolio_catalog"


@pytest.fixture(scope="session")
def warehouse_http_path():
    """SQL warehouse HTTP path for SQL-based tests."""
    return "/sql/1.0/warehouses/05c2aaf223ab7e44"
