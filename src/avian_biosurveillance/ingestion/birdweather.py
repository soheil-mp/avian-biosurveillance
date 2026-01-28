#!/usr/bin/env python3
"""BirdWeather Data Ingestion Module.

A comprehensive data ingestion pipeline for the BirdWeather API, designed to fetch
and persist bird detection data, station metadata, species taxonomy, and associated
audio recordings for biosurveillance research applications.

This module provides both programmatic access via the `BirdWeatherClient` and
`DataIngestor` classes, as well as a command-line interface for batch ingestion jobs.

Features
--------
- **Full Data Fidelity**: Captures all fields from the GraphQL API including station
  sensors, environmental data, species taxonomy, and detection confidence metrics.
- **Resilient Fetching**: Implements exponential backoff, HTTP 429/5xx retry logic,
  and connection pooling for robust operation against rate limits.
- **Cursor-Based Pagination**: Efficiently streams millions of records without
  memory exhaustion using generator-based iteration.
- **Flexible Filtering**: Supports geographic bounding boxes, date ranges, quality
  thresholds (score/confidence), recording modes, and time-of-day constraints.
- **Audio Downloads**: Optionally downloads raw audio files with size validation
  and automatic resume for interrupted transfers.
- **Incremental Ingestion**: Persists data incrementally to enable resumption
  after failures without re-fetching completed records.

Usage
-----
Command-line interface::

    $ python -m avian_biosurveillance.ingestion.birdweather --days 7 --min-score 0.8
    $ python -m avian_biosurveillance.ingestion.birdweather \\
        --start-date 2024-01-01 --end-date 2024-01-31 --download-audio

Programmatic usage::

    >>> from avian_biosurveillance.ingestion.birdweather import DataIngestor
    >>> from pathlib import Path
    >>> ingestor = DataIngestor(Path('data/raw/birdweather'))
    >>> ingestor.run(
    ...     ne={'lat': 53.7, 'lon': 7.22},
    ...     sw={'lat': 50.75, 'lon': 3.33},
    ...     days=7
    ... )

API Reference
-------------
BirdWeather GraphQL API: https://app.birdweather.com/api/index.html

See Also
--------
- BirdNET: https://birdnet.cornell.edu/
- eBird: https://ebird.org/
"""

import argparse
import json
import logging
import time
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Dict, List, Optional, Any, Generator, Tuple

import requests

# --- Configuration & Constants ---

API_ENDPOINT = "https://app.birdweather.com/graphql"

# Default Bounding Box: Netherlands
DEFAULT_NE = {"lat": 53.7, "lon": 7.22}
DEFAULT_SW = {"lat": 50.75, "lon": 3.33}

HEADERS = {
    "User-Agent": "AvianBiosurveillance/1.0 (Research Project; contact@example.com)",
    "Content-Type": "application/json",
    "Accept": "application/json",
}

# --- Detailed GraphQL Queries ---

QUERY_STATIONS_COMPREHENSIVE = """
query Stations($ne: InputLocation, $sw: InputLocation, $cursor: String, $query: String) {
  stations(
    ne: $ne
    sw: $sw
    query: $query
    first: 50
    after: $cursor
  ) {
    pageInfo { hasNextPage endCursor }
    totalCount
    nodes {
      id
      name
      location
      location
      timezone
      continent
      country
      notes
      audioUrl
      
      coords { lat lon }
      

      # Environmental & Status (Legacy & New)
      airPollution {
        aqi
        timestamp
      }
      
      weather {
        temp
        humidity
        pressure
        windSpeed
        windDir
        windGust
        timestamp
      }
      
      sensors {
        environment {
          temperature
          humidity
          timestamp
        }
        # Fetching a small history to capture trends without overloading
        environmentHistory(first: 24) { 
            nodes { temperature humidity timestamp } 
        }
        light {
          timestamp
        }
        lightHistory(first: 24) {
            nodes { timestamp }
        }
        accel {
          x
          y
          z
          timestamp
        }
        system {
          batteryVoltage
          wifiRssi
          timestamp
        }
      }
      
      # Aggregates
      counts {
        species
        detections
      }
    }
  }
}
"""

QUERY_DETECTIONS_COMPREHENSIVE = """
query Detections(
  $stationIds: [ID!], 
  $start: ISO8601DateTime, 
  $end: ISO8601DateTime, 
  $cursor: String,
  $minScore: Float,
  $minConfidence: Float,
  $validSoundscape: Boolean,
  $recordingModes: [String!],
  $eclipse: Boolean,
  $timeOfDayGte: Int,
  $timeOfDayLte: Int,
  $countries: [String!],
  $continents: [String!]
) {
  detections(
    stationIds: $stationIds
    period: { start: $start, end: $end }
    scoreGte: $minScore
    confidenceGte: $minConfidence
    validSoundscape: $validSoundscape
    recordingModes: $recordingModes
    eclipse: $eclipse
    timeOfDayGte: $timeOfDayGte
    timeOfDayLte: $timeOfDayLte
    countries: $countries
    continents: $continents
    first: 100
    after: $cursor
    order: ASC
  ) {
    pageInfo { hasNextPage endCursor }
    totalCount
    nodes {
      id
      timestamp
      cursor: timestamp
      
      # Classification Metrics
      confidence
      score
      probability
      certainty
      
      # Taxonomy
      species {
        id
        commonName
        scientificName
        commonName
        scientificName
        ebirdCode
      }
      
      # Links & flags
      favoriteUrl
      flagUrl
      voteUrl
      eclipse
      
      # Geolocation
      coords { lat lon }
      
      # Audio / Soundscape
      soundscape {
        id
        url
        downloadFilename
        duration
        filesize
        startTime
        endTime
        mode
        timestamp
      }
      
      # Context
      speciesId
      station {
        id
        name
        country
      }
    }
  }
}
"""

QUERY_SPECIES_FULL = """
query SpeciesList($cursor: String) {
  searchSpecies(query: "", first: 100, after: $cursor) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id
      commonName
      scientificName
      scientificName
      alpha
      alpha6
      ebirdCode
      birdweatherUrl
      ebirdUrl
      wikipediaUrl
      wikipediaSummary
      imageUrl
      imageCredit
      imageLicense
      
    }
  }
}
"""

QUERY_BIRDNET_SIGHTINGS = """
query BirdnetSightings(
  $cursor: String,
  $start: ISO8601DateTime,
  $end: ISO8601DateTime,
  $ne: InputLocation,
  $sw: InputLocation
) {
  birdnetSightings(
    first: 100
    after: $cursor
    period: { start: $start, end: $end }
    ne: $ne
    sw: $sw
  ) {
    pageInfo { hasNextPage endCursor }
    totalCount
    nodes {
      id
      timestamp
      score
      uncertainty: certainty
      location
      coords { lat lon }
      species {
        id
        commonName
        scientificName

      }
      speciesId
    }
  }
}

"""

QUERY_COUNTS = """
query Counts($period: InputDuration) {
  counts(period: $period) {
    detections
    species
    stations
    birdnet
    breakdown {
      stations {
        type
        count
      }
    }
  }
}
"""

QUERY_DAILY_DETECTION_COUNTS = """
query DailyDetectionCounts($period: InputDuration) {
  dailyDetectionCounts(period: $period) {
    counts {
      species {
         id
         commonName
      }
      count
    }
    date
    dayOfYear
  }
}
"""


# --- Logging ---

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


# --- Client & Logic ---

@dataclass
class BirdWeatherClient:
    """A robust HTTP client for interacting with the BirdWeather GraphQL API.

    This client provides resilient data fetching capabilities with automatic retry
    logic, exponential backoff for rate limiting, and connection pooling via
    requests.Session objects.

    Attributes:
        session: HTTP session for GraphQL API requests with JSON headers.
        max_retries: Maximum number of retry attempts for failed requests.
        base_backoff: Base delay in seconds for exponential backoff calculation.
        download_session: Separate HTTP session for downloading audio files.

    Example:
        >>> client = BirdWeatherClient()
        >>> for station in client.fetch_all_stations(ne, sw):
        ...     print(station['name'])
    """
    
    session: requests.Session = field(default_factory=requests.Session)
    max_retries: int = 5
    base_backoff: float = 2.0
    download_session: requests.Session = field(default_factory=requests.Session)

    def __post_init__(self):
        self.session.headers.update(HEADERS)
        # Download session doesn't need JSON headers
        self.download_session.headers.update({
            "User-Agent": HEADERS["User-Agent"]
        })

    def _execute_query(self, query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Execute a GraphQL query against the BirdWeather API with automatic retry logic.

        Implements resilient query execution with exponential backoff for transient
        failures including rate limiting (HTTP 429) and server errors (HTTP 5xx).

        Args:
            query: The GraphQL query string to execute.
            variables: Optional dictionary of variables to pass with the query.

        Returns:
            The JSON response data from the API as a dictionary.

        Raises:
            Exception: If max retries are exceeded or a non-recoverable error occurs.
            requests.RequestException: For network-level failures after max retries.
        """
        payload = {"query": query, "variables": variables or {}}
        
        for attempt in range(self.max_retries):
            try:
                response = self.session.post(API_ENDPOINT, json=payload, timeout=60)
                
                if response.status_code == 429:
                    sleep_time = self.base_backoff * (2 ** attempt)
                    logger.warning(f"Rate limited (429). Retrying in {sleep_time}s...")
                    time.sleep(sleep_time)
                    continue
                
                if response.status_code >= 500:
                    sleep_time = self.base_backoff * (2 ** attempt)
                    logger.warning(f"Server error ({response.status_code}). Retrying in {sleep_time}s...")
                    time.sleep(sleep_time)
                    continue
                
                try:
                    data = response.json()
                except json.JSONDecodeError:
                    if attempt == self.max_retries - 1:
                        raise Exception("Invalid JSON response from server")
                    continue

                if "errors" in data:
                    logger.error(f"GraphQL Errors: {data['errors']}")
                    if not data.get("data"):
                        raise Exception(f"GraphQL Error: {data['errors']}")
                
                return data

            except requests.RequestException as e:
                if attempt == self.max_retries - 1:
                    raise e
                logger.warning(f"Network error: {e}. Retrying...")
                time.sleep(self.base_backoff * (2 ** attempt))
        
        raise Exception("Max retries exceeded")

    def download_file(self, url: str, path: Path, expected_size: Optional[int] = None) -> bool:
        """Download a file from a URL with optional size validation.

        Downloads files with streaming to handle large audio files efficiently.
        Automatically creates parent directories and validates file integrity
        using expected file size when provided.

        Args:
            url: The URL of the file to download.
            path: The local filesystem path where the file should be saved.
            expected_size: Optional expected file size in bytes for validation.
                If provided and a file of matching size already exists, the
                download is skipped.

        Returns:
            True if the file was successfully downloaded or already exists
            with valid size, False if the download failed.

        Note:
            Partial downloads are automatically cleaned up on failure.
        """
        try:
            # Check if exists and valid
            if path.exists():
                if expected_size and path.stat().st_size == expected_size:
                    return True # Already downloaded
                if not expected_size and path.stat().st_size > 0:
                    return True # Assume valid if no size provided
            
            # Download
            path.parent.mkdir(parents=True, exist_ok=True)
            with self.download_session.get(url, stream=True, timeout=30) as r:
                r.raise_for_status()
                with open(path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            
            return True
        except Exception as e:
            logger.error(f"Failed to download audio {url}: {e}")
            if path.exists():
                path.unlink() # Delete partial
            return False

    def fetch_all_stations(self, ne: Dict, sw: Dict, query: str = None) -> Generator[Dict, None, None]:
        """Fetch all BirdWeather stations within a geographic bounding box.

        Streams station data using cursor-based pagination to efficiently handle
        large result sets without loading all data into memory at once.

        Args:
            ne: Northeast corner coordinates as {'lat': float, 'lon': float}.
            sw: Southwest corner coordinates as {'lat': float, 'lon': float}.
            query: Optional search string to filter stations by name.

        Yields:
            Dict: Station data including id, name, location, sensors, and counts.

        Example:
            >>> for station in client.fetch_all_stations(
            ...     ne={'lat': 53.7, 'lon': 7.22},
            ...     sw={'lat': 50.75, 'lon': 3.33}
            ... ):
            ...     print(f"{station['name']}: {station['counts']['detections']} detections")
        """
        cursor = None
        current_count = 0
        while True:
            vars = {"ne": ne, "sw": sw, "cursor": cursor, "query": query}
            data = self._execute_query(QUERY_STATIONS_COMPREHENSIVE, vars)
            
            connection = data.get("data", {}).get("stations", {})
            nodes = connection.get("nodes", [])
            page_info = connection.get("pageInfo", {})
            
            for node in nodes:
                yield node
                current_count += 1
            
            if not page_info.get("hasNextPage"):
                logger.info(f"Finished fetching stations. Total: {current_count}")
                break
            
            cursor = page_info.get("endCursor")
            time.sleep(0.2)

    def fetch_detections(self, station_ids: Dict, start: datetime, end: datetime, 
                         min_score: float = None, min_confidence: float = None,
                         valid_soundscape: bool = None, recording_modes: List[str] = None,
                         eclipse: bool = None,
                         time_of_day_gte: int = None, time_of_day_lte: int = None,
                         countries: List[str] = None, continents: List[str] = None) -> Generator[Dict, None, None]:
        """Fetch bird detections from specified stations within a time range.

        Streams detection records with comprehensive filtering options for
        quality control and temporal/geographic constraints.

        Args:
            station_ids: List of station IDs to query for detections.
            start: Start datetime for the query period (inclusive).
            end: End datetime for the query period (exclusive).
            min_score: Minimum BirdNET detection score threshold (0.0-1.0).
            min_confidence: Minimum classification confidence threshold (0.0-1.0).
            valid_soundscape: If True, only return detections with valid audio.
            recording_modes: Filter by recording mode(s), e.g., ['continuous', 'triggered'].
            eclipse: If True, only return detections flagged during eclipse events.
            time_of_day_gte: Minimum time of day in minutes from midnight.
            time_of_day_lte: Maximum time of day in minutes from midnight.
            countries: Filter by country ISO codes or names.
            continents: Filter by continent names.

        Yields:
            Dict: Detection record including species, confidence scores, timestamp,
                coordinates, and associated soundscape metadata.
        """
        cursor = None
        iso_start = start.isoformat()
        iso_end = end.isoformat()
        
        while True:
            vars = {
                "stationIds": station_ids, 
                "start": iso_start, 
                "end": iso_end, 
                "cursor": cursor,
                "minScore": min_score,
                "minConfidence": min_confidence,
                "validSoundscape": valid_soundscape,
                "recordingModes": recording_modes,
                "eclipse": eclipse,
                "timeOfDayGte": time_of_day_gte,
                "timeOfDayLte": time_of_day_lte,
                "countries": countries,
                "continents": continents
            }
            data = self._execute_query(QUERY_DETECTIONS_COMPREHENSIVE, vars)
            
            connection = data.get("data", {}).get("detections", {})
            nodes = connection.get("nodes", [])
            page_info = connection.get("pageInfo", {})
            
            if not nodes and not cursor:
                break

            for node in nodes:
                yield node
            
            if not page_info.get("hasNextPage"):
                break
            
            cursor = page_info.get("endCursor")
            time.sleep(0.1)

    def fetch_all_species(self) -> Generator[Dict, None, None]:
        """Fetch the complete BirdWeather species taxonomy database.

        Streams all species records from the BirdWeather database including
        taxonomic information, external identifiers, and media references.

        Yields:
            Dict: Species record containing:
                - id: BirdWeather species identifier
                - commonName, scientificName: Species names
                - ebirdCode, alpha, alpha6: External taxonomy codes
                - wikipediaUrl, wikipediaSummary: Reference information
                - imageUrl, imageCredit, imageLicense: Species imagery
        """
        cursor = None
        while True:
            vars = {"cursor": cursor}
            data = self._execute_query(QUERY_SPECIES_FULL, vars)
            
            connection = data.get("data", {}).get("species", {})
            nodes = connection.get("nodes", [])
            page_info = connection.get("pageInfo", {})
            
            for node in nodes:
                yield node
            
            if not page_info.get("hasNextPage"):
                break
            
            cursor = page_info.get("endCursor")
            time.sleep(0.1)

    def fetch_data_counts(self, period: Dict, ne: Dict = None, sw: Dict = None) -> Dict:
        """Fetch aggregate detection and station counts for a time period.

        Retrieves summary statistics including total detections, species counts,
        active stations, and BirdNET sightings for the specified period.

        Args:
            period: Time period as {'from': ISO8601, 'to': ISO8601}.
            ne: Optional northeast bounding box corner (currently unused).
            sw: Optional southwest bounding box corner (currently unused).

        Returns:
            Dict containing:
                - detections: Total detection count
                - species: Unique species count
                - stations: Active station count
                - birdnet: BirdNET sighting count
                - breakdown: Detailed station type breakdown
        """
        vars = {"period": period}
        data = self._execute_query(QUERY_COUNTS, vars)
        return data.get("data", {}).get("counts", {})

    def fetch_daily_detection_counts(self, period: Dict, ne: Dict = None, sw: Dict = None) -> List[Dict]:
        """Fetch daily aggregated detection counts broken down by species.

        Retrieves per-day detection statistics useful for temporal analysis
        and species activity pattern visualization.

        Args:
            period: Time period as {'from': ISO8601, 'to': ISO8601}.
            ne: Optional northeast bounding box corner (currently unused).
            sw: Optional southwest bounding box corner (currently unused).

        Returns:
            List of daily count records, each containing:
                - date: The date in ISO format
                - dayOfYear: Day of year (1-366)
                - counts: List of {species: {...}, count: int} entries
        """
        vars = {"period": period}
        data = self._execute_query(QUERY_DAILY_DETECTION_COUNTS, vars)
        return data.get("data", {}).get("dailyDetectionCounts", [])

# --- Ingestion Management ---

class DataIngestor:
    """Orchestrates the complete BirdWeather data ingestion pipeline.

    Manages the end-to-end process of fetching, transforming, and persisting
    BirdWeather data including stations, detections, species metadata, and
    optional audio files.

    Attributes:
        output_dir: Base directory for all output files.
        client: BirdWeatherClient instance for API communication.

    Example:
        >>> ingestor = DataIngestor(Path('data/raw/birdweather'))
        >>> ingestor.run(ne={'lat': 53.7, 'lon': 7.22}, sw={'lat': 50.75, 'lon': 3.33}, days=7)
    """

    def __init__(self, output_dir: Path):
        """Initialize the data ingestor with an output directory.

        Args:
            output_dir: Path to the directory where ingested data will be stored.
                Creates the directory structure if it doesn't exist.
        """
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.client = BirdWeatherClient()

    def ingest_metadata(self) -> None:
        """Ingest the complete species taxonomy metadata from BirdWeather.

        Downloads and persists the full species database including scientific
        names, common names, external identifiers (eBird codes), and media
        references to a JSONL file.

        Output:
            Creates 'species_metadata.jsonl' in the output directory with
            one JSON record per line, each containing an '_ingested_at' timestamp.
        """
        logger.info("Ingesting Species Metadata...")
        filename = self.output_dir / "species_metadata.jsonl"
        count = 0
        with open(filename, "w", encoding="utf-8") as f:
            for sp in self.client.fetch_all_species():
                sp["_ingested_at"] = datetime.utcnow().isoformat()
                f.write(json.dumps(sp) + "\n")
                count += 1
        logger.info(f"Ingested {count} species records.")
        
    def ingest_aggregates(self, ne: Dict, sw: Dict, days: int) -> None:
        """Ingest aggregate statistics including global counts and daily breakdowns.

        Fetches summary-level data for dashboard displays and trend analysis,
        including total counts and per-day species detection frequencies.

        Args:
            ne: Northeast corner coordinates as {'lat': float, 'lon': float}.
            sw: Southwest corner coordinates as {'lat': float, 'lon': float}.
            days: Number of days to look back from the current date.

        Output:
            Creates two files in the output directory:
                - counts_YYYY-MM-DD.json: Global aggregate counts
                - daily_counts_YYYY-MM-DD.jsonl: Per-day detection breakdown
        """
        logger.info("Ingesting Aggregate Data...")
        
        # Define period
        end = datetime.utcnow()
        start = end - timedelta(days=days)
        period = {"from": start.isoformat(), "to": end.isoformat()}
        
        # 1. Global Counts
        counts = self.client.fetch_data_counts(period, ne, sw)
        counts["_ingested_at"] = datetime.utcnow().isoformat()
        counts["period"] = period
        
        counts_file = self.output_dir / f"counts_{date.today()}.json"
        with open(counts_file, "w", encoding="utf-8") as f:
            json.dump(counts, f, indent=2)
            
        # 2. Daily Detection Counts
        daily_counts = self.client.fetch_daily_detection_counts(period, ne, sw)
        daily_file = self.output_dir / f"daily_counts_{date.today()}.jsonl"
        with open(daily_file, "w", encoding="utf-8") as f:
            for dc in daily_counts:
                dc["_ingested_at"] = datetime.utcnow().isoformat()
                f.write(json.dumps(dc) + "\n")
                
        logger.info("Finished Aggregate Data.")

    def fetch_birdnet_sightings(self, ne: Dict, sw: Dict, start: datetime, end: datetime) -> Generator[Dict, None, None]:
        """Fetch BirdNET mobile app sightings within a geographic region.

        Streams community-contributed bird sightings from the BirdNET mobile
        application (distinct from station-based detections) for citizen
        science data integration.

        Args:
            ne: Northeast corner coordinates as {'lat': float, 'lon': float}.
            sw: Southwest corner coordinates as {'lat': float, 'lon': float}.
            start: Start datetime for the query period (inclusive).
            end: End datetime for the query period (exclusive).

        Yields:
            Dict: BirdNET sighting record including:
                - id, timestamp: Unique identifier and observation time
                - species: Identified bird species information
                - score, uncertainty: Classification confidence metrics
                - coords, location: Geographic coordinates and place name
        """
        cursor = None
        iso_start = start.isoformat()
        iso_end = end.isoformat()
        
        while True:
            vars = {
                "ne": ne, 
                "sw": sw, 
                "start": iso_start, 
                "end": iso_end,
                "cursor": cursor
            }
            data = self._execute_query(QUERY_BIRDNET_SIGHTINGS, vars)
            
            connection = data.get("data", {}).get("birdnetSightings", {})
            nodes = connection.get("nodes", [])
            page_info = connection.get("pageInfo", {})
            
            for node in nodes:
                yield node
            
            if not page_info.get("hasNextPage"):
                break
            
            cursor = page_info.get("endCursor")
            time.sleep(0.1)

    def run(self, ne: Dict, sw: Dict, days: int, start_date: Optional[str], end_date: Optional[str],
            min_score: Optional[float], min_confidence: Optional[float], skip_stations: bool = False,
            valid_soundscape: bool = None, recording_modes: List[str] = None, eclipse: bool = None,
            download_audio: bool = False, ingest_birdnet: bool = False, ingest_aggregates: bool = False,
            time_of_day_gte: int = None, time_of_day_lte: int = None,
            countries: List[str] = None, continents: List[str] = None,
            station_query: str = None) -> None:
        """Execute the complete data ingestion pipeline.

        Orchestrates the multi-stage ingestion process including station discovery,
        detection fetching, optional audio downloads, and auxiliary data sources.
        Data is persisted incrementally to enable resumption after failures.

        Args:
            ne: Northeast corner coordinates as {'lat': float, 'lon': float}.
            sw: Southwest corner coordinates as {'lat': float, 'lon': float}.
            days: Number of days to look back (used if start/end dates not provided).
            start_date: Optional start date as 'YYYY-MM-DD' string.
            end_date: Optional end date as 'YYYY-MM-DD' string.
            min_score: Minimum BirdNET detection score threshold (0.0-1.0).
            min_confidence: Minimum classification confidence threshold (0.0-1.0).
            skip_stations: If True, load stations from existing local file.
            valid_soundscape: If True, only fetch detections with valid audio.
            recording_modes: Filter by recording mode(s), e.g., ['continuous'].
            eclipse: If True, only fetch detections flagged during eclipse events.
            download_audio: If True, download raw audio files for each detection.
            ingest_birdnet: If True, also ingest BirdNET mobile app sightings.
            ingest_aggregates: If True, also ingest aggregate count statistics.
            time_of_day_gte: Minimum time of day in minutes from midnight.
            time_of_day_lte: Maximum time of day in minutes from midnight.
            countries: Filter by country ISO codes or names.
            continents: Filter by continent names.
            station_query: Optional search string to filter stations by name.

        Output:
            Creates dated files in the output directory:
                - stations_YYYY-MM-DD.jsonl: Station metadata
                - detections_{station_id}_{date}.jsonl: Per-station detections
                - audio/{station_id}/{date}/: Downloaded audio files (if enabled)
                - birdnet_sightings_YYYY-MM-DD.jsonl: BirdNET data (if enabled)
        """
        
        # -1. Aggregates (New Source)
        if ingest_aggregates:
            self.ingest_aggregates(ne, sw, days)
        if start_date and end_date:
            start = datetime.strptime(start_date, "%Y-%m-%d")
            end = datetime.strptime(end_date, "%Y-%m-%d")
        else:
            end = datetime.now()
            start = end - timedelta(days=days)

        if ingest_birdnet:
            logger.info("Step 0: Ingesting BirdNET Sightings...")
            bn_file = self.output_dir / f"birdnet_sightings_{date.today()}.jsonl"
            with open(bn_file, "w", encoding="utf-8") as f:
                for sighting in self.client.fetch_birdnet_sightings(ne, sw, start, end):
                     sighting["_ingested_at"] = datetime.utcnow().isoformat()
                     f.write(json.dumps(sighting) + "\n")
            logger.info("Finished BirdNET Sightings.")

        # 1. Stations
        stations = []
        if not skip_stations:
            logger.info("Step 1: Ingesting Stations...")
            stat_file = self.output_dir / f"stations_{date.today()}.jsonl"
            with open(stat_file, "w", encoding="utf-8") as f:
                for st in self.client.fetch_all_stations(ne, sw, query=station_query):
                    st["_ingested_at"] = datetime.utcnow().isoformat()
                    stations.append(st)
                    f.write(json.dumps(st) + "\n")
            logger.info(f"Found {len(stations)} stations.")
        else:
            stat_file = self.output_dir / f"stations_{date.today()}.jsonl"
            if stat_file.exists():
                logger.info("Loading stations from existing file...")
                with open(stat_file, "r", encoding="utf-8") as f:
                    stations = [json.loads(line) for line in f]
            else:
                logger.error("Cannot skip station ingestion: no local station file found.")
                return

        # 2. Detections
        if start_date and end_date:
            start = datetime.strptime(start_date, "%Y-%m-%d")
            end = datetime.strptime(end_date, "%Y-%m-%d")
        else:
            end = datetime.now()
            start = end - timedelta(days=days)
        
        logger.info(f"Step 2: Ingesting Detections from {start.date()} to {end.date()}...")
        
        for station in stations:
            sid = station["id"]
            sname = station.get("name", "unknown")
            
            last_active = station.get("updatedAt")
            if last_active:
                last_active_dt = datetime.fromisoformat(last_active.replace("Z", "+00:00"))
                if last_active_dt.date() < start.date():
                    logger.debug(f"Skipping {sname} (last active {last_active})")
                    continue

            curr_d = start
            while curr_d < end:
                next_d = curr_d + timedelta(days=1)
                day_str = curr_d.strftime("%Y-%m-%d")
                
                out_file = self.output_dir / f"detections_{sid}_{day_str}.jsonl"
                # If file exists, we might still want to digest it for audio downloading
                # if download_audio is True. But for now, let's assume if JSONL exists, we are done
                # unless a specific --force-audio flag was added. 
                # To be simple: if download_audio is ON, we iterate the data source again OR read the local file.
                # Let's read the local file if it exists to save API calls.
                
                detections_to_process = []
                
                if out_file.exists():
                    logger.info(f"Reading local detections for {sname} on {day_str}")
                    with open(out_file, "r", encoding="utf-8") as f:
                        detections_to_process = [json.loads(line) for line in f]
                else:
                    logger.info(f"Fetching {sname} ({sid}) for {day_str}")
                    try:
                        temp_file = out_file.with_suffix(".tmp")
                        with open(temp_file, "w", encoding="utf-8") as f:
                            for det in self.client.fetch_detections(
                                [sid], curr_d, next_d, 
                                min_score, min_confidence, 
                                valid_soundscape, recording_modes, eclipse,
                                time_of_day_gte, time_of_day_lte,
                                countries, continents
                            ):
                                det["station_id"] = sid
                                f.write(json.dumps(det) + "\n")
                                detections_to_process.append(det)
                        
                        if detections_to_process:
                            temp_file.rename(out_file)
                            logger.info(f"-> Saved {len(detections_to_process)} detections.")
                        else:
                            temp_file.unlink()
                            out_file.touch()
                            logger.info("-> No detections.")

                    except Exception as e:
                        logger.error(f"Failed station {sid} on day {day_str}: {e}")
                
                # 3. Audio Download (Optional)
                if download_audio and detections_to_process:
                    audio_dir = self.output_dir / "audio" / str(sid) / day_str
                    logger.info(f"-> Downloading {len(detections_to_process)} audio files to {audio_dir}...")
                    
                    for det in detections_to_process:
                        soundscape = det.get("soundscape")
                        if not soundscape:
                            continue
                            
                        url = soundscape.get("url")
                        if not url:
                            continue
                            
                        # Try to use downloadFilename if available, else construct one
                        filename = soundscape.get("downloadFilename")
                        if not filename:
                            filename = f"{det['id']}.mp3" # Fallback
                            
                        path = audio_dir / filename
                        expected_size = soundscape.get("filesize")
                        
                        self.client.download_file(url, path, expected_size)
                
                curr_d = next_d

# --- CLI Entrypoint ---

def main() -> None:
    """Command-line interface entrypoint for the BirdWeather data ingestion pipeline.

    Parses command-line arguments and executes the appropriate ingestion tasks
    based on user-specified options for geographic filtering, date ranges,
    quality thresholds, and output preferences.

    Example:
        $ python birdweather.py --days 7 --min-score 0.8 --download-audio
        $ python birdweather.py --start-date 2024-01-01 --end-date 2024-01-31
    """
    parser = argparse.ArgumentParser(description="BirdWeather Comprehensive Ingestion")
    parser.add_argument("--output-dir", type=str, default="data/raw/birdweather", help="Data root")
    parser.add_argument("--days", type=int, default=7, help="Days to look back")
    parser.add_argument("--start-date", type=str, help="YYYY-MM-DD")
    parser.add_argument("--end-date", type=str, help="YYYY-MM-DD")
    
    # Filtering / Quality
    parser.add_argument("--min-score", type=float, help="Minimum detection score")
    parser.add_argument("--min-confidence", type=float, help="Minimum confidence")
    parser.add_argument("--valid-soundscape", action="store_true", help="Only fetch detections with valid soundscapes")
    parser.add_argument("--recording-mode", type=str, action="append", help="Filter by recording mode (e.g. 'continuous', 'triggered')")
    parser.add_argument("--eclipse", action="store_true", help="Only fetch detections flagged as eclipse")
    
    # New Filters
    parser.add_argument("--time-of-day-gte", type=int, help="Min time of day (minutes from midnight?)")
    parser.add_argument("--time-of-day-lte", type=int, help="Max time of day")
    parser.add_argument("--country", type=str, action="append", help="Limit to country (ISO code or name)")
    parser.add_argument("--continent", type=str, action="append", help="Limit to continent")
    parser.add_argument("--station-query", type=str, help="Search string for stations")

    # Audio Data
    parser.add_argument("--download-audio", action="store_true", help="Download raw audio files for each detection")

    # Bounding Box (Default Netherlands)
    parser.add_argument("--lat-ne", type=float, default=DEFAULT_NE["lat"])
    parser.add_argument("--lon-ne", type=float, default=DEFAULT_NE["lon"])
    parser.add_argument("--lat-sw", type=float, default=DEFAULT_SW["lat"])
    parser.add_argument("--lon-sw", type=float, default=DEFAULT_SW["lon"])
    
    # Flags
    parser.add_argument("--ingest-species", action="store_true", help="Download full species database")
    parser.add_argument("--ingest-birdnet", action="store_true", help="Ingest BirdNET sightings (non-station)")
    parser.add_argument("--ingest-aggregates", action="store_true", help="Ingest aggregate counts and daily stats")
    parser.add_argument("--skip-stations", action="store_true", help="Use existing stations file")
    
    args = parser.parse_args()
    
    ingestor = DataIngestor(Path(args.output_dir))
    
    if args.ingest_species:
        ingestor.ingest_metadata()
    
    ne = {"lat": args.lat_ne, "lon": args.lon_ne}
    sw = {"lat": args.lat_sw, "lon": args.lon_sw}
    
    ingestor.run(
        ne=ne, sw=sw, 
        days=args.days, 
        start_date=args.start_date, 
        end_date=args.end_date,
        min_score=args.min_score,
        min_confidence=args.min_confidence,
        skip_stations=args.skip_stations,
        valid_soundscape=args.valid_soundscape if args.valid_soundscape else None,
        recording_modes=args.recording_mode,
        eclipse=args.eclipse if args.eclipse else None,
        download_audio=args.download_audio,
        ingest_birdnet=args.ingest_birdnet,
        ingest_aggregates=args.ingest_aggregates,
        time_of_day_gte=args.time_of_day_gte,
        time_of_day_lte=args.time_of_day_lte,
        countries=args.country,
        continents=args.continent,
        station_query=args.station_query
    )

if __name__ == "__main__":
    main()
