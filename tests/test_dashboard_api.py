"""Tests for the dashboard FastAPI application endpoints."""

import pytest
from fastapi.testclient import TestClient

from dashboard.app import app


@pytest.fixture
def client():
    return TestClient(app)


def test_health_endpoint(client):
    res = client.get("/health")
    assert res.status_code == 200
    assert res.json()["status"] == "healthy"


def test_ui_dashboard(client):
    res = client.get("/")
    assert res.status_code == 200
    assert "Streaming Iceberg Lakehouse" in res.text


def test_ingest_endpoint(client):
    res = client.post("/v1/ingest?count=20")
    assert res.status_code == 200
    data = res.json()
    assert data["accepted"] > 0
    assert "watermark" in data


def test_compact_endpoint(client):
    res = client.post("/v1/compact")
    assert res.status_code == 200
    assert "files_after" in res.json()


def test_purge_endpoint(client):
    res = client.post("/v1/gdpr/purge", json={"user_id": "non_existent_user"})
    assert res.status_code == 200
