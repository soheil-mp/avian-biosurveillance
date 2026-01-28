"""
KNMI Weather Data Ingestion Module
==================================
Downloads weather observations from the Royal Netherlands Meteorological Institute.
Provides environmental covariates for acoustic analysis.
"""

import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .base import BaseIngestionModule, DataRecord

logger = logging.getLogger(__name__)


# KNMI station metadata for Netherlands
# Selected stations providing good geographic coverage
KNMI_STATIONS = {
    "260": {"name": "De Bilt", "lat": 52.10, "lon": 5.18, "province": "Utrecht"},
    "240": {"name": "Schiphol", "lat": 52.30, "lon": 4.77, "province": "Noord-Holland"},
    "344": {"name": "Rotterdam", "lat": 51.96, "lon": 4.45, "province": "Zuid-Holland"},
    "370": {"name": "Eindhoven", "lat": 51.45, "lon": 5.42, "province": "Noord-Brabant"},
    "380": {"name": "Maastricht", "lat": 50.91, "lon": 5.77, "province": "Limburg"},
    "270": {"name": "Leeuwarden", "lat": 53.22, "lon": 5.75, "province": "Friesland"},
    "280": {"name": "Eelde", "lat": 53.13, "lon": 6.58, "province": "Groningen"},
    "290": {"name": "Twenthe", "lat": 52.27, "lon": 6.90, "province": "Overijssel"},
    "375": {"name": "Volkel", "lat": 51.66, "lon": 5.71, "province": "Noord-Brabant"},
    "350": {"name": "Gilze-Rijen", "lat": 51.57, "lon": 4.93, "province": "Noord-Brabant"},
}

# Variable mapping: KNMI code -> standardized name
VARIABLE_MAPPING = {
    "T": "temperature_c",           # Temperature in 0.1 degrees Celsius
    "T10N": "temp_min_10cm_c",      # Minimum temperature at 10cm
    "TX": "temp_max_c",             # Maximum temperature
    "TN": "temp_min_c",             # Minimum temperature  
    "RH": "precipitation_mm",        # Hourly precipitation in 0.1 mm
    "FF": "wind_speed_ms",          # Wind speed in 0.1 m/s
    "FX": "wind_gust_ms",           # Maximum wind gust
    "DD": "wind_direction_deg",     # Wind direction in degrees
    "U": "humidity_pct",            # Relative humidity in %
    "P": "pressure_hpa",            # Air pressure in 0.1 hPa
    "N": "cloud_cover_oktas",       # Cloud cover in oktas (0-9)
    "SQ": "sunshine_hours",         # Sunshine duration in 0.1 hours
    "DR": "precipitation_duration", # Precipitation duration in 0.1 hours
}


class KNMIWeatherIngestion(BaseIngestionModule):
    """
    Ingestion module for KNMI weather data.
    
    The KNMI Open Data API provides historical and real-time weather
    observations from meteorological stations across the Netherlands.
    
    API Documentation: https://dataplatform.knmi.nl/
    """
    
    BASE_URL = "https://api.dataplatform.knmi.nl/open-data/v1"
    
    def __init__(
        self,
        config: Dict[str, Any],
        output_path: Path,
        checkpoint_path: Optional[Path] = None
    ):
        super().__init__(
            source_id="knmi_weather",
            config=config,
            output_path=output_path,
            checkpoint_path=checkpoint_path
        )
        
        self.api_key = config.get("api_key", "")
        self.stations = config.get("stations", list(KNMI_STATIONS.keys()))
        self.variables = config.get("variables", list(VARIABLE_MAPPING.keys()))
        self.rate_limit_delay = config.get("rate_limit_delay", 0.5)
        
        self.session = self._create_session()
    
    def _create_session(self) -> requests.Session:
        """Create HTTP session with retry configuration."""
        session = requests.Session()
        
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        
        if self.api_key:
            session.headers["Authorization"] = self.api_key
        
        session.headers["User-Agent"] = "AvianBiosurveillance/1.0"
        
        return session
    
    def connect(self) -> bool:
        """Test connection to KNMI API."""
        try:
            # Test with datasets listing endpoint
            response = self.session.get(
                f"{self.BASE_URL}/datasets",
                timeout=30
            )
            
            if response.status_code == 200:
                logger.info("Connected to KNMI Open Data API")
                return True
            elif response.status_code == 403:
                logger.warning("KNMI API key may be required for full access")
                return True  # Can still access some data
            else:
                logger.error(f"KNMI connection test failed: {response.status_code}")
                return False
                
        except requests.RequestException as e:
            logger.error(f"Connection error: {e}")
            return False
    
    def _fetch_hourly_data(
        self,
        station_id: str,
        start_date: datetime,
        end_date: datetime
    ) -> List[Dict[str, Any]]:
        """
        Fetch hourly weather data for a station.
        
        Note: KNMI provides data in specific file formats.
        This implementation uses the simplified JSON API where available.
        """
        # KNMI hourly data endpoint
        # Format: YYYYMMDDHH
        start_str = start_date.strftime("%Y%m%d")
        end_str = end_date.strftime("%Y%m%d")
        
        params = {
            "stns": station_id,
            "start": start_str,
            "end": end_str,
            "vars": ":".join(self.variables)
        }
        
        # Note: Actual KNMI API requires file-based downloads
        # This is a simplified implementation
        # In production, use the KNMI Data Platform file downloads
        
        url = f"{self.BASE_URL}/datasets/Actuele10teledag/versions/1/files"
        
        try:
            response = self.session.get(url, params=params, timeout=60)
            
            if response.status_code == 200:
                return response.json().get("files", [])
            else:
                logger.warning(f"KNMI request returned {response.status_code}")
                return []
                
        except requests.RequestException as e:
            logger.error(f"KNMI fetch error: {e}")
            return []
    
    def fetch(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        station_ids: Optional[List[str]] = None,
        **kwargs
    ) -> Generator[DataRecord, None, None]:
        """
        Fetch weather observation records.
        
        Args:
            start_date: Start of time range.
            end_date: End of time range.
            station_ids: Specific stations (None for all configured).
            
        Yields:
            DataRecord for each observation.
        """
        end_date = end_date or datetime.utcnow()
        start_date = start_date or (end_date - timedelta(days=7))
        stations_to_fetch = station_ids or self.stations
        
        logger.info(f"Fetching KNMI data for {len(stations_to_fetch)} stations, "
                   f"{start_date.date()} to {end_date.date()}")
        
        # Generate hourly observations for the date range
        # In a real implementation, this would parse KNMI file downloads
        current = start_date
        while current <= end_date:
            for station_id in stations_to_fetch:
                station_info = KNMI_STATIONS.get(station_id, {})
                
                # Create observation record
                observation = {
                    "station_id": station_id,
                    "station_name": station_info.get("name", "Unknown"),
                    "latitude": station_info.get("lat"),
                    "longitude": station_info.get("lon"),
                    "province": station_info.get("province"),
                    "timestamp": current.isoformat(),
                    "date": current.strftime("%Y-%m-%d"),
                    "hour": current.hour,
                    # Placeholder values - in production, parse actual KNMI data
                    "T": None,  # Temperature
                    "RH": None,  # Precipitation
                    "FF": None,  # Wind speed
                    "U": None,   # Humidity
                    "P": None,   # Pressure
                }
                
                yield DataRecord(
                    record_id=f"{station_id}_{current.strftime('%Y%m%d%H')}",
                    source_system="knmi_weather",
                    ingestion_timestamp=datetime.utcnow(),
                    raw_data=observation
                )
            
            current += timedelta(hours=1)
    
    def validate(self, record: DataRecord) -> DataRecord:
        """Validate KNMI weather record."""
        errors = []
        data = record.raw_data
        
        # Required fields
        if not data.get("station_id"):
            errors.append("Missing station_id")
        
        if not data.get("timestamp"):
            errors.append("Missing timestamp")
        
        # Coordinate validation
        lat = data.get("latitude")
        lon = data.get("longitude")
        
        if lat is not None:
            if not (50.0 <= lat <= 54.0):
                errors.append(f"Latitude {lat} outside Netherlands range")
        
        if lon is not None:
            if not (3.0 <= lon <= 8.0):
                errors.append(f"Longitude {lon} outside Netherlands range")
        
        # Value range checks (using KNMI scaling factors)
        temp = data.get("T")
        if temp is not None:
            # KNMI reports in 0.1°C, so actual range is -50 to +50°C
            if not (-500 <= temp <= 500):
                errors.append(f"Temperature {temp} outside valid range")
        
        humidity = data.get("U")
        if humidity is not None:
            if not (0 <= humidity <= 100):
                errors.append(f"Humidity {humidity} outside valid range")
        
        record.validated = len(errors) == 0
        record.validation_errors = errors
        
        return record
    
    def transform(self, record: DataRecord) -> Dict[str, Any]:
        """Transform KNMI record to standardized schema."""
        data = record.raw_data
        
        # Parse timestamp
        timestamp_str = data.get("timestamp", "")
        try:
            timestamp = datetime.fromisoformat(timestamp_str)
        except ValueError:
            timestamp = record.ingestion_timestamp
        
        # Apply KNMI scaling factors
        def scale_value(value, factor):
            if value is None:
                return None
            try:
                return float(value) / factor
            except (ValueError, TypeError):
                return None
        
        return {
            "observation_id": record.record_id,
            "timestamp": timestamp.isoformat(),
            "station_id": data.get("station_id"),
            "station_name": data.get("station_name"),
            "latitude": data.get("latitude"),
            "longitude": data.get("longitude"),
            "province": data.get("province"),
            
            # Scaled meteorological variables
            "temperature_c": scale_value(data.get("T"), 10),
            "temp_max_c": scale_value(data.get("TX"), 10),
            "temp_min_c": scale_value(data.get("TN"), 10),
            "precipitation_mm": scale_value(data.get("RH"), 10),
            "wind_speed_ms": scale_value(data.get("FF"), 10),
            "wind_gust_ms": scale_value(data.get("FX"), 10),
            "wind_direction_deg": data.get("DD"),
            "humidity_pct": data.get("U"),
            "pressure_hpa": scale_value(data.get("P"), 10),
            "cloud_cover_oktas": data.get("N"),
            "sunshine_hours": scale_value(data.get("SQ"), 10),
            
            # Metadata
            "source_system": "knmi_weather",
            "_raw_hash": record.compute_hash(),
            "_ingestion_timestamp": record.ingestion_timestamp.isoformat(),
            "_batch_id": self._batch_id
        }


def create_knmi_module(
    api_key: str = "",
    output_path: str = "./data",
    stations: Optional[List[str]] = None
) -> KNMIWeatherIngestion:
    """
    Factory function to create configured KNMI ingestion module.
    
    Args:
        api_key: KNMI API key (optional for basic access).
        output_path: Base path for output data.
        stations: List of station IDs (None for all default stations).
        
    Returns:
        Configured KNMIWeatherIngestion instance.
    """
    config = {
        "api_key": api_key,
        "stations": stations or list(KNMI_STATIONS.keys()),
        "variables": list(VARIABLE_MAPPING.keys()),
        "rate_limit_delay": 0.5
    }
    
    return KNMIWeatherIngestion(
        config=config,
        output_path=Path(output_path)
    )


if __name__ == "__main__":
    import os
    
    api_key = os.environ.get("KNMI_API_KEY", "")
    
    module = create_knmi_module(
        api_key=api_key,
        output_path="./data/biosurveillance"
    )
    
    # Fetch last 24 hours
    result = module.run(
        start_date=datetime.utcnow() - timedelta(hours=24),
        incremental=True
    )
    
    print(f"Ingestion completed: {result.to_dict()}")
