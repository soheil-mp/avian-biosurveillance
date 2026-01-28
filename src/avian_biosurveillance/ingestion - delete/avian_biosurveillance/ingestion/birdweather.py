"""
BirdWeather Ingestion Module
============================
Downloads real-time bird detection data from the BirdWeather platform.
Supports both polling and incremental fetch modes.
"""

import logging
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .base import BaseIngestionModule, DataRecord, IngestionStatus

logger = logging.getLogger(__name__)


# Netherlands bounding box for geographic validation
NETHERLANDS_BOUNDS = {
    "min_lat": 50.75,
    "max_lat": 53.47,
    "min_lon": 3.37,
    "max_lon": 7.21
}

# Target species for USUV surveillance (eBird codes)
TARGET_SPECIES = {
    "eurbla": "Eurasian Blackbird",
    "sonthr1": "Song Thrush",
    "eurgrn1": "European Greenfinch",
    "eurmag1": "Eurasian Magpie",
    "eurjay1": "Eurasian Jay",
    "comwoo1": "Common Wood-Pigeon",
    "eursta": "European Starling"
}


class BirdWeatherIngestion(BaseIngestionModule):
    """
    Ingestion module for BirdWeather API.
    
    BirdWeather provides real-time bird detection data from citizen science
    recording stations running BirdNET classifiers.
    
    API Documentation: https://app.birdweather.com/api/v1/
    """
    
    BASE_URL = "https://app.birdweather.com/api/v1"
    
    def __init__(
        self,
        config: Dict[str, Any],
        output_path: Path,
        checkpoint_path: Optional[Path] = None
    ):
        super().__init__(
            source_id="birdweather",
            config=config,
            output_path=output_path,
            checkpoint_path=checkpoint_path
        )
        
        self.api_token = config.get("api_token")
        self.rate_limit_delay = config.get("rate_limit_delay", 0.5)  # seconds between requests
        self.page_size = config.get("page_size", 100)
        self.target_species = config.get("target_species", list(TARGET_SPECIES.keys()))
        self.geographic_filter = config.get("geographic_filter", NETHERLANDS_BOUNDS)
        
        # Configure session with retry logic
        self.session = self._create_session()
    
    def _create_session(self) -> requests.Session:
        """Create HTTP session with retry configuration."""
        session = requests.Session()
        
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        
        if self.api_token:
            session.headers["Authorization"] = f"Bearer {self.api_token}"
        
        session.headers["User-Agent"] = "AvianBiosurveillance/1.0"
        
        return session
    
    def connect(self) -> bool:
        """Test connection to BirdWeather API."""
        try:
            response = self.session.get(
                f"{self.BASE_URL}/stations",
                params={"limit": 1},
                timeout=30
            )
            
            if response.status_code == 200:
                logger.info("Successfully connected to BirdWeather API")
                return True
            elif response.status_code == 401:
                logger.error("Authentication failed - check API token")
                return False
            else:
                logger.error(f"Connection test failed: {response.status_code}")
                return False
                
        except requests.RequestException as e:
            logger.error(f"Connection error: {e}")
            return False
    
    def _get_stations_in_region(self) -> List[Dict[str, Any]]:
        """Fetch list of stations within geographic bounds."""
        stations = []
        
        try:
            # BirdWeather doesn't have native bbox filtering, 
            # so we fetch all and filter client-side
            response = self.session.get(
                f"{self.BASE_URL}/stations",
                params={
                    "limit": 500,
                    "active": True
                },
                timeout=60
            )
            response.raise_for_status()
            
            all_stations = response.json().get("stations", [])
            
            # Filter to Netherlands bounds
            for station in all_stations:
                lat = station.get("latitude")
                lon = station.get("longitude")
                
                if lat and lon:
                    if (self.geographic_filter["min_lat"] <= lat <= self.geographic_filter["max_lat"] and
                        self.geographic_filter["min_lon"] <= lon <= self.geographic_filter["max_lon"]):
                        stations.append(station)
            
            logger.info(f"Found {len(stations)} stations in target region")
            
        except requests.RequestException as e:
            logger.error(f"Failed to fetch stations: {e}")
        
        return stations
    
    def fetch(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        station_ids: Optional[List[str]] = None,
        **kwargs
    ) -> Generator[DataRecord, None, None]:
        """
        Fetch detection records from BirdWeather.
        
        Args:
            start_date: Fetch detections after this time.
            end_date: Fetch detections before this time.
            station_ids: List of specific station IDs to query (None for all in region).
            
        Yields:
            DataRecord for each detection.
        """
        # Default time range: last 24 hours
        end_date = end_date or datetime.utcnow()
        start_date = start_date or (end_date - timedelta(hours=24))
        
        # Get stations to query
        if station_ids is None:
            stations = self._get_stations_in_region()
            station_ids = [s["id"] for s in stations]
        
        logger.info(f"Fetching detections from {len(station_ids)} stations, "
                   f"{start_date.isoformat()} to {end_date.isoformat()}")
        
        for station_id in station_ids:
            try:
                yield from self._fetch_station_detections(
                    station_id, start_date, end_date
                )
                time.sleep(self.rate_limit_delay)  # Rate limiting
                
            except Exception as e:
                logger.warning(f"Error fetching station {station_id}: {e}")
                continue
    
    def _fetch_station_detections(
        self,
        station_id: str,
        start_date: datetime,
        end_date: datetime
    ) -> Generator[DataRecord, None, None]:
        """Fetch all detections for a single station."""
        cursor = None
        
        while True:
            params = {
                "station_id": station_id,
                "from": start_date.isoformat() + "Z",
                "to": end_date.isoformat() + "Z",
                "limit": self.page_size
            }
            
            if cursor:
                params["cursor"] = cursor
            
            # Filter by species if specified
            if self.target_species:
                params["species"] = ",".join(self.target_species)
            
            response = self.session.get(
                f"{self.BASE_URL}/detections",
                params=params,
                timeout=60
            )
            
            if response.status_code == 429:
                # Rate limited - back off
                retry_after = int(response.headers.get("Retry-After", 60))
                logger.warning(f"Rate limited, waiting {retry_after}s")
                time.sleep(retry_after)
                continue
            
            response.raise_for_status()
            data = response.json()
            
            detections = data.get("detections", [])
            
            for detection in detections:
                yield DataRecord(
                    record_id=detection.get("id", ""),
                    source_system="birdweather",
                    ingestion_timestamp=datetime.utcnow(),
                    raw_data=detection
                )
            
            # Check for more pages
            cursor = data.get("next_cursor")
            if not cursor or len(detections) < self.page_size:
                break
            
            time.sleep(self.rate_limit_delay)
    
    def validate(self, record: DataRecord) -> DataRecord:
        """
        Validate a BirdWeather detection record.
        
        Checks:
        - Required fields present
        - Confidence score in valid range
        - Coordinates within Netherlands
        - Timestamp is valid and not future
        """
        errors = []
        data = record.raw_data
        
        # Required fields
        required_fields = ["id", "timestamp", "species", "confidence"]
        for field in required_fields:
            if field not in data or data[field] is None:
                errors.append(f"Missing required field: {field}")
        
        # Confidence range [0.01, 1.0]
        confidence = data.get("confidence")
        if confidence is not None:
            if not (0.01 <= confidence <= 1.0):
                errors.append(f"Confidence {confidence} outside valid range [0.01, 1.0]")
        
        # Geographic bounds
        lat = data.get("latitude")
        lon = data.get("longitude")
        
        if lat is not None and lon is not None:
            if not (self.geographic_filter["min_lat"] <= lat <= self.geographic_filter["max_lat"]):
                errors.append(f"Latitude {lat} outside Netherlands bounds")
            if not (self.geographic_filter["min_lon"] <= lon <= self.geographic_filter["max_lon"]):
                errors.append(f"Longitude {lon} outside Netherlands bounds")
        
        # Timestamp validation
        timestamp_str = data.get("timestamp")
        if timestamp_str:
            try:
                ts = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
                if ts > datetime.utcnow().replace(tzinfo=ts.tzinfo):
                    errors.append("Timestamp is in the future")
            except ValueError:
                errors.append(f"Invalid timestamp format: {timestamp_str}")
        
        record.validated = len(errors) == 0
        record.validation_errors = errors
        
        return record
    
    def transform(self, record: DataRecord) -> Dict[str, Any]:
        """
        Transform BirdWeather record to standardized schema.
        
        Maps raw BirdWeather fields to the Acoustic Detection Record schema.
        """
        data = record.raw_data
        
        # Parse timestamp
        timestamp_str = data.get("timestamp", "")
        try:
            timestamp = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
        except ValueError:
            timestamp = record.ingestion_timestamp
        
        # Get species information
        species_data = data.get("species", {})
        if isinstance(species_data, str):
            species_code = species_data
            species_common = TARGET_SPECIES.get(species_data, species_data)
        else:
            species_code = species_data.get("code", "")
            species_common = species_data.get("commonName", "")
        
        return {
            "detection_id": data.get("id"),
            "timestamp": timestamp.isoformat(),
            "station_id": data.get("station_id") or data.get("stationId"),
            "latitude": data.get("latitude"),
            "longitude": data.get("longitude"),
            "species_code": species_code,
            "species_common": species_common,
            "confidence_raw": data.get("confidence"),
            "confidence_calibrated": None,  # To be computed in silver layer
            "audio_url": data.get("soundscape", {}).get("url") if isinstance(data.get("soundscape"), dict) else data.get("soundscapeUrl"),
            "source_system": "birdweather",
            "birdnet_version": data.get("model", "unknown"),
            "temperature_c": data.get("temperature"),
            "humidity_pct": data.get("humidity"),
            "_raw_hash": record.compute_hash(),
            "_ingestion_timestamp": record.ingestion_timestamp.isoformat(),
            "_batch_id": self._batch_id
        }


def create_birdweather_module(
    api_token: str,
    output_path: str = "./data",
    target_species: Optional[List[str]] = None
) -> BirdWeatherIngestion:
    """
    Factory function to create configured BirdWeather ingestion module.
    
    Args:
        api_token: BirdWeather API token.
        output_path: Base path for output data.
        target_species: List of species codes to filter (None for defaults).
        
    Returns:
        Configured BirdWeatherIngestion instance.
    """
    config = {
        "api_token": api_token,
        "target_species": target_species or list(TARGET_SPECIES.keys()),
        "rate_limit_delay": 0.5,
        "page_size": 100,
        "geographic_filter": NETHERLANDS_BOUNDS
    }
    
    return BirdWeatherIngestion(
        config=config,
        output_path=Path(output_path)
    )


# Example usage
if __name__ == "__main__":
    import os
    
    # Get API token from environment
    api_token = os.environ.get("BIRDWEATHER_API_TOKEN", "")
    
    if not api_token:
        print("Set BIRDWEATHER_API_TOKEN environment variable")
        exit(1)
    
    # Create module
    module = create_birdweather_module(
        api_token=api_token,
        output_path="./data/biosurveillance"
    )
    
    # Run ingestion
    result = module.run(
        start_date=datetime.utcnow() - timedelta(hours=24),
        incremental=True
    )
    
    print(f"Ingestion completed: {result.to_dict()}")
