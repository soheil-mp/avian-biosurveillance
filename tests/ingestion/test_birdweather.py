
import pytest
from unittest.mock import MagicMock, patch, mock_open
import json
from datetime import datetime
from pathlib import Path
import sys
import os

# Add src to sys.path to ensure we can import the module if it's not installed
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../src')))

from avian_biosurveillance.ingestion.birdweather import (
    BirdWeatherClient, 
    DataIngestor,
    QUERY_COUNTS,
    QUERY_DAILY_DETECTION_COUNTS
)

@pytest.fixture
def mock_client():
    client = BirdWeatherClient()
    client._execute_query = MagicMock()
    return client

@pytest.fixture
def ingestor(tmp_path):
    with patch('avian_biosurveillance.ingestion.birdweather.BirdWeatherClient') as MockClient:
        mock_instance = MagicMock()
        MockClient.return_value = mock_instance
        ingestor = DataIngestor(output_dir=tmp_path)
        ingestor.client = mock_instance
        yield ingestor

def test_fetch_data_counts(mock_client):
    # Setup mock return
    mock_response = {
        "data": {
            "counts": {
                "detections": 100,
                "species": 10,
                "stations": 5
            }
        }
    }
    mock_client._execute_query.return_value = mock_response
    
    period = {"from": "2025-01-01", "to": "2025-01-08"}
    
    result = mock_client.fetch_data_counts(period)
    
    # Verify call
    mock_client._execute_query.assert_called_once()
    args, _ = mock_client._execute_query.call_args
    assert args[0] == QUERY_COUNTS
    assert args[1] == {"period": period}
    
    # Verify result
    assert result["detections"] == 100

def test_fetch_daily_detection_counts(mock_client):
    # Setup mock return
    mock_response = {
        "data": {
            "dailyDetectionCounts": [
                {"date": "2025-01-01", "total": 50},
                {"date": "2025-01-02", "total": 50}
            ]
        }
    }
    mock_client._execute_query.return_value = mock_response
    
    period = {"from": "2025-01-01", "to": "2025-01-08"}
    
    result = mock_client.fetch_daily_detection_counts(period)
    
    # Verify call
    mock_client._execute_query.assert_called_once()
    args, _ = mock_client._execute_query.call_args
    assert args[0] == QUERY_DAILY_DETECTION_COUNTS
    assert args[1] == {"period": period}
    
    # Verify result
    assert len(result) == 2
    assert result[0]["date"] == "2025-01-01"

def test_ingest_aggregates(ingestor):
    # Setup mocks on the ingestor's client
    ingestor.client.fetch_data_counts = MagicMock(return_value={"detections": 100})
    ingestor.client.fetch_daily_detection_counts = MagicMock(return_value=[{"date": "2025-01-01", "count": 50}])
    
    # Run
    # Provide dummy ne/sw to match method signature (though we removed them from queries)
    ingestor.ingest_aggregates(ne={}, sw={}, days=7)
    
    # Verify fetches
    ingestor.client.fetch_data_counts.assert_called_once()
    ingestor.client.fetch_daily_detection_counts.assert_called_once()
    
    # Check if period was constructed correctly (from/to)
    call_args = ingestor.client.fetch_data_counts.call_args
    period_arg = call_args[0][0]
    assert "from" in period_arg
    assert "to" in period_arg
    
    # Verify file creation
    # We expect counts_YYYY-MM-DD.json and daily_counts_YYYY-MM-DD.jsonl
    # Since filename relies on date.today(), we check directory content
    files = list(ingestor.output_dir.glob("counts_*.json"))
    assert len(files) == 1
    
    with open(files[0], 'r') as f:
        data = json.load(f)
        assert data["detections"] == 100
        assert "_ingested_at" in data
        
    daily_files = list(ingestor.output_dir.glob("daily_counts_*.jsonl"))
    assert len(daily_files) == 1
    
    with open(daily_files[0], 'r') as f:
        line = f.readline()
        item = json.loads(line)
        assert item["date"] == "2025-01-01"
        assert "_ingested_at" in item
