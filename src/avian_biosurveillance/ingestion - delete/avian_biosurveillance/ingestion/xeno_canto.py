"""
Xeno-Canto Ingestion Module
===========================
Downloads labeled bird vocalization recordings from the Xeno-Canto repository.
Used for BirdNET training data and validation corpus.
"""

import logging
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional
from urllib.parse import urljoin
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .base import BaseIngestionModule, DataRecord

logger = logging.getLogger(__name__)


# USUV-susceptible species for targeted download
USUV_SUSCEPTIBLE_SPECIES = [
    {"scientific": "Turdus merula", "common": "Eurasian Blackbird"},
    {"scientific": "Turdus philomelos", "common": "Song Thrush"},
    {"scientific": "Turdus viscivorus", "common": "Mistle Thrush"},
    {"scientific": "Pica pica", "common": "Eurasian Magpie"},
    {"scientific": "Garrulus glandarius", "common": "Eurasian Jay"},
    {"scientific": "Chloris chloris", "common": "European Greenfinch"},
    {"scientific": "Sturnus vulgaris", "common": "European Starling"},
    {"scientific": "Corvus corone", "common": "Carrion Crow"},
    {"scientific": "Cyanistes caeruleus", "common": "Eurasian Blue Tit"},
    {"scientific": "Parus major", "common": "Great Tit"},
]


class XenoCantoIngestion(BaseIngestionModule):
    """
    Ingestion module for Xeno-Canto bird sound repository.
    
    Xeno-Canto is a citizen-science collection of bird vocalizations
    used for training and validating species classification models.
    
    API Documentation: https://xeno-canto.org/explore/api
    """
    
    BASE_URL = "https://xeno-canto.org/api/2/recordings"
    
    def __init__(
        self,
        config: Dict[str, Any],
        output_path: Path,
        checkpoint_path: Optional[Path] = None
    ):
        super().__init__(
            source_id="xeno_canto",
            config=config,
            output_path=output_path,
            checkpoint_path=checkpoint_path
        )
        
        self.target_species = config.get("target_species", USUV_SUSCEPTIBLE_SPECIES)
        self.country_filter = config.get("country_filter", "Netherlands")
        self.quality_filter = config.get("quality_filter", "A")  # A, B, C, D, E
        self.download_audio = config.get("download_audio", True)
        self.rate_limit_delay = config.get("rate_limit_delay", 1.0)
        
        self.audio_output = self.output_path / "raw_audio" / "xeno_canto"
        if self.download_audio:
            self.audio_output.mkdir(parents=True, exist_ok=True)
        
        self.session = self._create_session()
    
    def _create_session(self) -> requests.Session:
        """Create HTTP session with retry configuration."""
        session = requests.Session()
        
        retry_strategy = Retry(
            total=3,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        
        session.headers["User-Agent"] = "AvianBiosurveillance/1.0 (research project)"
        
        return session
    
    def connect(self) -> bool:
        """Test connection to Xeno-Canto API."""
        try:
            response = self.session.get(
                self.BASE_URL,
                params={"query": "cnt:Netherlands", "page": 1},
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                total = data.get("numRecordings", 0)
                logger.info(f"Connected to Xeno-Canto. {total} Netherlands recordings available.")
                return True
            else:
                logger.error(f"Connection test failed: {response.status_code}")
                return False
                
        except requests.RequestException as e:
            logger.error(f"Connection error: {e}")
            return False
    
    def _build_query(self, species: Dict[str, str]) -> str:
        """Build Xeno-Canto query string for a species."""
        parts = []
        
        # Species name
        parts.append(species["scientific"])
        
        # Country filter
        if self.country_filter:
            parts.append(f"cnt:{self.country_filter}")
        
        # Quality filter (A is best, E is worst)
        if self.quality_filter:
            quality_options = ["A", "B", "C", "D", "E"]
            idx = quality_options.index(self.quality_filter.upper())
            acceptable = quality_options[:idx + 1]
            parts.append(f"q:{','.join(acceptable)}")
        
        return " ".join(parts)
    
    def fetch(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        species_list: Optional[List[Dict[str, str]]] = None,
        **kwargs
    ) -> Generator[DataRecord, None, None]:
        """
        Fetch recording metadata from Xeno-Canto.
        
        Args:
            start_date: Filter recordings uploaded after this date.
            end_date: Filter recordings uploaded before this date.
            species_list: Override default species list.
            
        Yields:
            DataRecord for each recording.
        """
        species_to_fetch = species_list or self.target_species
        
        for species in species_to_fetch:
            logger.info(f"Fetching recordings for {species['scientific']}")
            
            try:
                yield from self._fetch_species_recordings(species, start_date, end_date)
            except Exception as e:
                logger.error(f"Error fetching {species['scientific']}: {e}")
                continue
            
            time.sleep(self.rate_limit_delay)
    
    def _fetch_species_recordings(
        self,
        species: Dict[str, str],
        start_date: Optional[datetime],
        end_date: Optional[datetime]
    ) -> Generator[DataRecord, None, None]:
        """Fetch all recordings for a single species."""
        query = self._build_query(species)
        page = 1
        
        while True:
            response = self.session.get(
                self.BASE_URL,
                params={"query": query, "page": page},
                timeout=60
            )
            
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 60))
                logger.warning(f"Rate limited, waiting {retry_after}s")
                time.sleep(retry_after)
                continue
            
            response.raise_for_status()
            data = response.json()
            
            recordings = data.get("recordings", [])
            
            for recording in recordings:
                # Date filter (upload date)
                uploaded = recording.get("uploaded")
                if uploaded:
                    try:
                        upload_date = datetime.strptime(uploaded, "%Y-%m-%d")
                        if start_date and upload_date < start_date:
                            continue
                        if end_date and upload_date > end_date:
                            continue
                    except ValueError:
                        pass
                
                # Add species info to raw data
                recording["_species_scientific"] = species["scientific"]
                recording["_species_common"] = species["common"]
                
                yield DataRecord(
                    record_id=recording.get("id", ""),
                    source_system="xeno_canto",
                    ingestion_timestamp=datetime.utcnow(),
                    raw_data=recording
                )
            
            # Check pagination
            num_pages = int(data.get("numPages", 1))
            if page >= num_pages:
                break
            
            page += 1
            time.sleep(self.rate_limit_delay)
    
    def validate(self, record: DataRecord) -> DataRecord:
        """
        Validate a Xeno-Canto recording record.
        
        Checks:
        - Required metadata fields
        - Audio file URL accessible
        - Quality rating acceptable
        """
        errors = []
        data = record.raw_data
        
        # Required fields
        required = ["id", "gen", "sp", "file"]
        for field in required:
            if field not in data or not data[field]:
                errors.append(f"Missing required field: {field}")
        
        # Quality check
        quality = data.get("q", "E")
        quality_order = ["A", "B", "C", "D", "E"]
        if self.quality_filter:
            max_idx = quality_order.index(self.quality_filter.upper())
            curr_idx = quality_order.index(quality.upper()) if quality.upper() in quality_order else 4
            if curr_idx > max_idx:
                errors.append(f"Quality {quality} below threshold {self.quality_filter}")
        
        # Location validation (if present)
        lat = data.get("lat")
        lng = data.get("lng")
        
        if lat and lng:
            try:
                lat_f = float(lat)
                lng_f = float(lng)
                if not (-90 <= lat_f <= 90):
                    errors.append(f"Invalid latitude: {lat}")
                if not (-180 <= lng_f <= 180):
                    errors.append(f"Invalid longitude: {lng}")
            except (ValueError, TypeError):
                errors.append("Invalid coordinate format")
        
        record.validated = len(errors) == 0
        record.validation_errors = errors
        
        return record
    
    def transform(self, record: DataRecord) -> Dict[str, Any]:
        """Transform Xeno-Canto record to standardized schema."""
        data = record.raw_data
        
        # Parse coordinates
        lat = None
        lon = None
        try:
            if data.get("lat"):
                lat = float(data["lat"])
            if data.get("lng"):
                lon = float(data["lng"])
        except (ValueError, TypeError):
            pass
        
        # Parse date
        recording_date = data.get("date", "")
        
        # Build audio URL
        file_url = data.get("file", "")
        if file_url and not file_url.startswith("http"):
            file_url = f"https:{file_url}"
        
        return {
            "recording_id": f"XC{data.get('id', '')}",
            "source_system": "xeno_canto",
            "species_scientific": data.get("_species_scientific") or f"{data.get('gen', '')} {data.get('sp', '')}",
            "species_common": data.get("_species_common") or data.get("en", ""),
            "latitude": lat,
            "longitude": lon,
            "country": data.get("cnt", ""),
            "location_name": data.get("loc", ""),
            "recording_date": recording_date,
            "recording_time": data.get("time", ""),
            "quality_rating": data.get("q", ""),
            "length_seconds": data.get("length", ""),
            "audio_url": file_url,
            "sonogram_url": data.get("sono", {}).get("full") if isinstance(data.get("sono"), dict) else None,
            "recordist": data.get("rec", ""),
            "remarks": data.get("rmk", ""),
            "vocalization_type": data.get("type", ""),
            "license": data.get("lic", ""),
            "uploaded_date": data.get("uploaded", ""),
            "_raw_hash": record.compute_hash(),
            "_ingestion_timestamp": record.ingestion_timestamp.isoformat(),
            "_batch_id": self._batch_id
        }
    
    def download_audio_file(self, audio_url: str, recording_id: str) -> Optional[Path]:
        """
        Download audio file for a recording.
        
        Args:
            audio_url: URL of the audio file.
            recording_id: Recording identifier for filename.
            
        Returns:
            Path to downloaded file, or None if failed.
        """
        if not self.download_audio:
            return None
        
        try:
            response = self.session.get(audio_url, stream=True, timeout=120)
            response.raise_for_status()
            
            # Determine file extension
            content_type = response.headers.get("content-type", "")
            if "mpeg" in content_type:
                ext = ".mp3"
            elif "wav" in content_type:
                ext = ".wav"
            else:
                ext = ".mp3"  # Default
            
            output_file = self.audio_output / f"{recording_id}{ext}"
            
            with open(output_file, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            logger.info(f"Downloaded: {output_file}")
            return output_file
            
        except Exception as e:
            logger.error(f"Failed to download {audio_url}: {e}")
            return None


def create_xenocanto_module(
    output_path: str = "./data",
    country: str = "Netherlands",
    quality: str = "B",
    download_audio: bool = False
) -> XenoCantoIngestion:
    """
    Factory function to create configured Xeno-Canto ingestion module.
    
    Args:
        output_path: Base path for output data.
        country: Country filter for recordings.
        quality: Minimum quality rating (A=best to E=worst).
        download_audio: Whether to download audio files.
        
    Returns:
        Configured XenoCantoIngestion instance.
    """
    config = {
        "target_species": USUV_SUSCEPTIBLE_SPECIES,
        "country_filter": country,
        "quality_filter": quality,
        "download_audio": download_audio,
        "rate_limit_delay": 1.0
    }
    
    return XenoCantoIngestion(
        config=config,
        output_path=Path(output_path)
    )


if __name__ == "__main__":
    # Create module
    module = create_xenocanto_module(
        output_path="./data/biosurveillance",
        country="Netherlands",
        quality="B",
        download_audio=False  # Set True to download audio files
    )
    
    # Run ingestion for training data
    result = module.run(incremental=False)
    
    print(f"Ingestion completed: {result.to_dict()}")
