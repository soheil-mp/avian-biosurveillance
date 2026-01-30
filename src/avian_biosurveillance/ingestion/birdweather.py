#!/usr/bin/env python3
"""BirdWeather Data Ingestion Module.
    
This module provides a comprehensive data ingestion pipeline for the BirdWeather API.
It includes full API coverage, bug fixes, and support for all available data endpoints
including real-time subscriptions, station management, and detection analytics.

Key Features:
    - **Comprehensive API Coverage**: Support for all BirdWeather GraphQL endpoints.
    - **Robust Error Handling**: Automatic retries, rate limiting, and backoff strategies.
    - **Real-time Subscriptions**: WebSocket support for live detection events.
    - **Data Ingestion**: Tools for bulk downloading stations, species, and detections.
    - **Audio/Image Download**: Parallel downloading of media files with resume capability.
    - **Analytics**: Methods for fetching aggregated counts and time-series data.

Notes:
    - Fixes period parameter format (from/to instead of start/end).
    - Fixes species query response parsing and BirdNET sightings.
    - Adds missing API endpoints (timeOfDayDetectionCounts, topSpecies, etc.).

Example:
    To collect data for the last 24 hours:

    .. code-block:: bash

        python -m avian_biosurveillance.ingestion.birdweather --hours 24
"""

import argparse
import json
import logging
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timedelta, date
from pathlib import Path
from queue import Queue
from typing import Dict, List, Optional, Any, Generator, Callable, Tuple

import requests

# --- Configuration & Constants ---

API_ENDPOINT = "https://app.birdweather.com/graphql"

# Default Bounding Box: Netherlands
DEFAULT_NE = {"lat": 53.7, "lon": 7.22}
DEFAULT_SW = {"lat": 50.75, "lon": 3.33}

HEADERS = {
    "User-Agent": "AvianBiosurveillance/2.0 (Research Project; contact@example.com)",
    "Content-Type": "application/json",
    "Accept": "application/json",
}

# --- Comprehensive GraphQL Queries ---

# ============================================================================
# STATIONS QUERIES
# ============================================================================

QUERY_STATIONS_COMPREHENSIVE = """
query Stations($ne: InputLocation, $sw: InputLocation, $cursor: String, $query: String, $period: InputDuration) {
  stations(
    ne: $ne
    sw: $sw
    query: $query
    period: $period
    first: 50
    after: $cursor
  ) {
    pageInfo { hasNextPage endCursor }
    totalCount
    nodes {
      id
      name
      type
      location
      timezone
      continent
      country
      state
      notes
      audioUrl
      videoUrl
      source
      
      coords { lat lon }
      
      # Station settings
      locationPrivacy
      portableDetections
      hasProbabilities
      minConfidence
      minProbability
      minScore
      eclipse
      
      # Timestamps
      earliestDetectionAt
      latestDetectionAt
      
      # Environmental Data
      airPollution {
        aqi
        co
        nh3
        no
        no2
        o3
        pm10
        pm2_5
        so2
        timestamp
        coords { lat lon }
      }
      
      weather {
        temp
        tempMin
        tempMax
        feelsLike
        humidity
        pressure
        seaLevel
        groundLevel
        cloudiness
        visibility
        windSpeed
        windDir
        windGust
        rain1h
        rain3h
        snow1h
        snow3h
        sunrise
        sunset
        description
        timestamp
        coords { lat lon }
      }
      
      # Sensor Data
      sensors {
        environment {
          temperature
          humidity
          barometricPressure
          aqi
          voc
          eco2
          soundPressureLevel
          timestamp
        }
        environmentHistory(first: 48) { 
          nodes { 
            temperature 
            humidity 
            barometricPressure
            aqi
            voc
            eco2
            soundPressureLevel
            timestamp 
          } 
        }
        light {
          clear
          nir
          f1 f2 f3 f4 f5 f6 f7 f8
          timestamp
        }
        lightHistory(first: 48) {
          nodes { 
            clear nir f1 f2 f3 f4 f5 f6 f7 f8 timestamp 
          }
        }
        accel {
          x y z
          timestamp
        }
        location {
          lat lon altitude satellites
          timestamp
        }
        locationHistory(first: 48) {
          nodes { lat lon altitude satellites timestamp }
        }
        mag {
          x y z
          timestamp
        }
        system {
          batteryVoltage
          wifiRssi
          firmware
          powerSource
          sdAvailable
          sdCapacity
          uploadingCompleted
          uploadingTotal
          timestamp
        }
        systemHistory(first: 48) {
          nodes {
            batteryVoltage wifiRssi firmware powerSource
            sdAvailable sdCapacity uploadingCompleted uploadingTotal timestamp
          }
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

QUERY_STATION_SINGLE = """
query Station($id: ID!, $period: InputDuration, $topSpeciesLimit: Int, $sensorHistoryLimit: Int, $detectionsFirst: Int) {
  station(id: $id) {
    id
    name
    type
    location
    timezone
    continent
    country
    state
    notes
    audioUrl
    videoUrl
    source
    
    coords { lat lon }
    
    locationPrivacy
    portableDetections
    hasProbabilities
    minConfidence
    minProbability
    minScore
    eclipse
    
    earliestDetectionAt
    latestDetectionAt
    
    airPollution {
      aqi co nh3 no no2 o3 pm10 pm2_5 so2 timestamp
      coords { lat lon }
    }
    
    weather {
      temp tempMin tempMax feelsLike humidity pressure
      seaLevel groundLevel cloudiness visibility
      windSpeed windDir windGust
      rain1h rain3h snow1h snow3h
      sunrise sunset description timestamp
      coords { lat lon }
    }
    
    counts(period: $period) {
      species
      detections
    }
    
    # Detection counts binned by time (NEW)
    detectionCounts(period: $period) {
      speciesId
      count
      species {
        id
        commonName
        scientificName
      }
      bins {
        key
        count
      }
    }
    
    # Time-of-day detection counts (NEW)
    timeOfDayDetectionCounts(period: $period) {
      speciesId
      count
      species {
        id
        commonName
        scientificName
      }
      bins {
        key
        count
      }
    }
    
    # Recent detections (NEW)
    detections(first: $detectionsFirst) {
      totalCount
      nodes {
        id
        timestamp
        score
        confidence
        probability
        certainty
        speciesId
        species {
          id
          commonName
          scientificName
        }
        soundscape {
          id
          url
          duration
        }
      }
    }
    
    # Weekly probability predictions per species
    probabilities {
      speciesId
      species {
        id
        commonName
        scientificName
      }
      weeks
    }
    
    # Top detected species at this station
    topSpecies(limit: $topSpeciesLimit, period: $period) {
      speciesId
      count
      averageProbability
      species {
        id
        commonName
        scientificName
        imageUrl
        thumbnailUrl
      }
      breakdown {
        almostCertain
        veryLikely
        uncertain
        unlikely
      }
    }
    
    # Full sensor data with configurable history limit
    sensors {
      environment { temperature humidity barometricPressure aqi voc eco2 soundPressureLevel timestamp }
      environmentHistory(first: $sensorHistoryLimit) {
        totalCount
        nodes { temperature humidity barometricPressure aqi voc eco2 soundPressureLevel timestamp }
      }
      light { clear nir f1 f2 f3 f4 f5 f6 f7 f8 timestamp }
      lightHistory(first: $sensorHistoryLimit) {
        totalCount
        nodes { clear nir f1 f2 f3 f4 f5 f6 f7 f8 timestamp }
      }
      accel { x y z timestamp }
      accelHistory(first: $sensorHistoryLimit) {
        totalCount
        nodes { x y z timestamp }
      }
      location { lat lon altitude satellites timestamp }
      locationHistory(first: $sensorHistoryLimit) {
        totalCount
        nodes { lat lon altitude satellites timestamp }
      }
      mag { x y z timestamp }
      magHistory(first: $sensorHistoryLimit) {
        totalCount
        nodes { x y z timestamp }
      }
      system { batteryVoltage wifiRssi firmware powerSource sdAvailable sdCapacity uploadingCompleted uploadingTotal timestamp }
      systemHistory(first: $sensorHistoryLimit) {
        totalCount
        nodes { batteryVoltage wifiRssi firmware powerSource sdAvailable sdCapacity uploadingCompleted uploadingTotal timestamp }
      }
    }
  }
}
"""

# ============================================================================
# DETECTIONS QUERIES
# ============================================================================

QUERY_DETECTIONS_COMPREHENSIVE = """
query Detections(
  $stationIds: [ID!], 
  $period: InputDuration,
  $cursor: String,
  # Score filters (all variants)
  $scoreGt: Float,
  $scoreLt: Float,
  $scoreGte: Float,
  $scoreLte: Float,
  # Confidence filters (all variants)
  $confidenceGt: Float,
  $confidenceLt: Float,
  $confidenceGte: Float,
  $confidenceLte: Float,
  # Probability filters (all variants)
  $probabilityGt: Float,
  $probabilityLt: Float,
  $probabilityGte: Float,
  $probabilityLte: Float,
  # Other filters
  $validSoundscape: Boolean,
  $recordingModes: [String!],
  $eclipse: Boolean,
  $timeOfDayGte: Int,
  $timeOfDayLte: Int,
  $countries: [String!],
  $continents: [String!],
  $speciesId: ID,
  $speciesIds: [ID!],
  $classifications: [String!],
  $stationTypes: [String!],
  $vote: Int,
  $sortBy: String,
  $uniqueStations: Boolean,
  $overrideStationFilters: Boolean,
  $ne: InputLocation,
  $sw: InputLocation
) {
  detections(
    stationIds: $stationIds
    period: $period
    scoreGt: $scoreGt
    scoreLt: $scoreLt
    scoreGte: $scoreGte
    scoreLte: $scoreLte
    confidenceGt: $confidenceGt
    confidenceLt: $confidenceLt
    confidenceGte: $confidenceGte
    confidenceLte: $confidenceLte
    probabilityGt: $probabilityGt
    probabilityLt: $probabilityLt
    probabilityGte: $probabilityGte
    probabilityLte: $probabilityLte
    validSoundscape: $validSoundscape
    recordingModes: $recordingModes
    eclipse: $eclipse
    timeOfDayGte: $timeOfDayGte
    timeOfDayLte: $timeOfDayLte
    countries: $countries
    continents: $continents
    speciesId: $speciesId
    speciesIds: $speciesIds
    classifications: $classifications
    stationTypes: $stationTypes
    vote: $vote
    sortBy: $sortBy
    uniqueStations: $uniqueStations
    overrideStationFilters: $overrideStationFilters
    ne: $ne
    sw: $sw
    first: 20
    after: $cursor
  ) {
    pageInfo { hasNextPage endCursor }
    totalCount
    speciesCount
    nodes {
      id
      timestamp
      
      # Classification Metrics
      confidence
      score
      probability
      certainty
      eclipse
      
      # Taxonomy
      speciesId
      species {
        id
        commonName
        scientificName
        alpha
        alpha6
        ebirdCode
        color
        imageUrl
        thumbnailUrl
      }
      
      # Links & flags
      favoriteUrl
      flagUrl
      voteUrl
      
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
        timestamp
      }
      
      # Station Context
      station {
        id
        name
        type
        country
        state
        coords { lat lon }
      }
    }
  }
}
"""

# ============================================================================
# SPECIES QUERIES
# ============================================================================

QUERY_SPECIES_SEARCH = """
query SearchSpecies($cursor: String, $query: String, $searchLocale: String, $limit: Int) {
  searchSpecies(query: $query, first: 100, after: $cursor, searchLocale: $searchLocale, limit: $limit) {
    pageInfo { hasNextPage endCursor }
    totalCount
    nodes {
      id
      commonName
      scientificName
      alpha
      alpha6
      ebirdCode
      color
      birdweatherUrl
      ebirdUrl
      mlUrl
      wikipediaUrl
      wikipediaSummary
      imageUrl
      thumbnailUrl
      imageCredit
      imageLicense
      imageLicenseUrl
      predictionArea
      range
    }
  }
}
"""

QUERY_SPECIES_SINGLE = """
query Species($id: ID, $scientificName: String, $period: InputDuration, $stationTypes: [String!]) {
  species(id: $id, scientificName: $scientificName) {
    id
    commonName
    scientificName
    alpha
    alpha6
    ebirdCode
    color
    birdweatherUrl
    ebirdUrl
    mlUrl
    wikipediaUrl
    wikipediaSummary
    imageUrl
    thumbnailUrl
    imageCredit
    imageLicense
    imageLicenseUrl
    predictionArea
    range
    
    # Translations for localized names
    translations {
      locale
      commonName
      wikipediaSummary
      wikipediaUrl
    }
    
    # Detection counts over time (binned)
    detectionCounts(period: $period) {
      speciesId
      count
      bins {
        key
        count
      }
    }
    
    # Which stations detected this species
    stations(period: $period, stationTypes: $stationTypes) {
      count
      station {
        id
        name
        country
        coords { lat lon }
      }
    }
    
    # Top quality detections for this species
    topDetections(period: $period, first: 10) {
      totalCount
      nodes {
        id
        timestamp
        score
        confidence
        probability
        soundscape {
          id
          url
          duration
        }
        station {
          id
          name
        }
      }
    }
  }
}
"""

QUERY_ALL_SPECIES = """
query AllSpecies($ids: [ID!]!, $cursor: String) {
  allSpecies(ids: $ids, first: 100, after: $cursor) {
    pageInfo { hasNextPage endCursor }
    totalCount
    nodes {
      id
      commonName
      scientificName
      alpha
      alpha6
      ebirdCode
      color
      imageUrl
      thumbnailUrl
      birdweatherUrl
      ebirdUrl
      wikipediaUrl
    }
  }
}
"""

# ============================================================================
# BIRDNET SIGHTINGS QUERY (Fixed)
# ============================================================================

QUERY_BIRDNET_SIGHTINGS = """
query BirdnetSightings(
  $cursor: String,
  $period: InputDuration,
  $ne: InputLocation,
  $sw: InputLocation,
  $speciesId: ID,
  $limit: Int,
  $offset: Int
) {
  birdnetSightings(
    first: 100
    after: $cursor
    period: $period
    ne: $ne
    sw: $sw
    speciesId: $speciesId
    limit: $limit
    offset: $offset
  ) {
    pageInfo { hasNextPage endCursor }
    totalCount
    nodes {
      id
      timestamp
      score
      coords { lat lon }
      speciesId
      species {
        id
        commonName
        scientificName
        alpha
        ebirdCode
      }
    }
  }
}
"""

# ============================================================================
# AGGREGATE / ANALYTICS QUERIES
# ============================================================================

QUERY_COUNTS = """
query Counts($period: InputDuration, $speciesId: ID, $stationIds: [ID!], $stationTypes: [String!], $ne: InputLocation, $sw: InputLocation) {
  counts(
    period: $period
    speciesId: $speciesId
    stationIds: $stationIds
    stationTypes: $stationTypes
    ne: $ne
    sw: $sw
  ) {
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
query DailyDetectionCounts($period: InputDuration, $speciesIds: [ID!], $stationIds: [ID!]) {
  dailyDetectionCounts(period: $period, speciesIds: $speciesIds, stationIds: $stationIds) {
    date
    dayOfYear
    total
    counts {
      speciesId
      count
      species {
        id
        commonName
        scientificName
      }
    }
  }
}
"""

QUERY_TIME_OF_DAY_DETECTION_COUNTS = """
query TimeOfDayDetectionCounts(
  $period: InputDuration,
  $speciesId: ID,
  $stationIds: [ID!],
  $scoreGte: Float,
  $confidenceGte: Float,
  $probabilityGte: Float,
  $timeOfDayGte: Int,
  $timeOfDayLte: Int,
  $ne: InputLocation,
  $sw: InputLocation
) {
  timeOfDayDetectionCounts(
    period: $period
    speciesId: $speciesId
    stationIds: $stationIds
    scoreGte: $scoreGte
    confidenceGte: $confidenceGte
    probabilityGte: $probabilityGte
    timeOfDayGte: $timeOfDayGte
    timeOfDayLte: $timeOfDayLte
    ne: $ne
    sw: $sw
  ) {
    speciesId
    count
    species {
      id
      commonName
      scientificName
    }
    bins {
      key
      count
    }
  }
}
"""

QUERY_TOP_SPECIES = """
query TopSpecies(
  $limit: Int,
  $offset: Int,
  $period: InputDuration,
  $speciesId: ID,
  $stationTypes: [String!],
  $stationIds: [ID!],
  $ne: InputLocation,
  $sw: InputLocation
) {
  topSpecies(
    limit: $limit
    offset: $offset
    period: $period
    speciesId: $speciesId
    stationTypes: $stationTypes
    stationIds: $stationIds
    ne: $ne
    sw: $sw
  ) {
    speciesId
    count
    averageProbability
    species {
      id
      commonName
      scientificName
      imageUrl
      thumbnailUrl
      ebirdCode
    }
    breakdown {
      almostCertain
      veryLikely
      uncertain
      unlikely
    }
  }
}
"""

QUERY_TOP_BIRDNET_SPECIES = """
query TopBirdnetSpecies(
  $limit: Int,
  $offset: Int,
  $period: InputDuration,
  $speciesId: ID,
  $ne: InputLocation,
  $sw: InputLocation
) {
  topBirdnetSpecies(
    limit: $limit
    offset: $offset
    period: $period
    speciesId: $speciesId
    ne: $ne
    sw: $sw
  ) {
    speciesId
    count
    averageProbability
    species {
      id
      commonName
      scientificName
      imageUrl
      thumbnailUrl
    }
    breakdown {
      almostCertain
      veryLikely
      uncertain
      unlikely
    }
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


# --- Download Statistics ---

@dataclass
class DownloadStats:
    """Track statistics for file download operations.
    
    Attributes:
        total (int): Total number of files processed.
        downloaded (int): Number of files successfully downloaded.
        already_exists (int): Number of files skipped because they already exist.
        failed (int): Number of files that failed to download.
        no_url (int): Number of items skipped because no download URL was provided.
        errors (Dict[str, int]): Dictionary mapping error types to their occurrence count.
    """
    total: int = 0
    downloaded: int = 0
    already_exists: int = 0
    failed: int = 0
    no_url: int = 0
    errors: Dict[str, int] = field(default_factory=dict)
    
    def add_error(self, error_type: str) -> None:
        """Record an error of the given type.
        
        Args:
            error_type (str): The category/name of the error to record.
        """
        self.errors[error_type] = self.errors.get(error_type, 0) + 1
    
    def summary(self) -> str:
        """Return a human-readable summary of download stats.
        
        Returns:
            str: Comma-separated summary string (e.g., "10 downloaded, 5 cached").
        """
        parts = []
        if self.downloaded:
            parts.append(f"{self.downloaded} downloaded")
        if self.already_exists:
            parts.append(f"{self.already_exists} cached")
        if self.failed:
            parts.append(f"{self.failed} failed")
        if self.no_url:
            parts.append(f"{self.no_url} no URL")
        return ", ".join(parts) if parts else "no files"
    
    def error_summary(self) -> str:
        """Return a summary of error types.
        
        Returns:
            str: Formatted string of error counts, or empty string if no errors.
        """
        if not self.errors:
            return ""
        return " | Errors: " + ", ".join(f"{k}: {v}" for k, v in self.errors.items())


# --- Client ---

@dataclass
class BirdWeatherClient:
    """A robust HTTP client for the BirdWeather GraphQL API.
    
    Handles authentication, error handling, retries, and rate limiting for
    both the GraphQL API and file downloads.
    
    Attributes:
        session (requests.Session): Session for API requests.
        max_retries (int): Maximum number of retries for failed requests.
        base_backoff (float): Base seconds for exponential backoff.
        download_session (requests.Session): Session for file downloads.
    """
    
    session: requests.Session = field(default_factory=requests.Session)
    max_retries: int = 5
    base_backoff: float = 2.0
    download_session: requests.Session = field(default_factory=requests.Session)

    def __post_init__(self):
        """Initialize headers and sessions."""
        self.session.headers.update(HEADERS)
        self.download_session.headers.update({"User-Agent": HEADERS["User-Agent"]})

    def _execute_query(self, query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Execute a GraphQL query with retry logic and error handling.
        
        Args:
            query (str): The GraphQL query string.
            variables (Optional[Dict[str, Any]]): Variables for the query.
            
        Returns:
            Dict[str, Any]: The JSON response data from the server.
            
        Raises:
            Exception: If max retries are exceeded or if the server returns GraphQL errors.
            requests.RequestException: For underlying network issues after retries.
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

    def download_file(
        self, 
        url: str, 
        path: Path, 
        expected_size: Optional[int] = None,
        max_retries: int = 3
    ) -> Tuple[bool, str]:
        """Download a file with retry logic and detailed status reporting.
        
        Args:
            url (str): URL to download from.
            path (Path): Local filesystem path to save the file.
            expected_size (Optional[int]): Expected file size in bytes for validation.
            max_retries (int): Maximum number of retry attempts.
            
        Returns:
            Tuple[bool, str]: A tuple containing (success, status).
            
            Status codes:
                - "downloaded": Successfully downloaded.
                - "already_exists": File already exists with correct size.
                - "not_found": 404 error.
                - "forbidden": 403 error.
                - "server_error": 5xx error.
                - "timeout": Request timed out.
                - "size_mismatch": Downloaded size doesn't match expected.
                - "error": Other generic error.
        """
        try:
            # Check if already downloaded
            if path.exists():
                actual_size = path.stat().st_size
                if expected_size and actual_size == expected_size:
                    return True, "already_exists"
                if not expected_size and actual_size > 0:
                    return True, "already_exists"
            
            path.parent.mkdir(parents=True, exist_ok=True)
            
            for attempt in range(max_retries):
                try:
                    with self.download_session.get(url, stream=True, timeout=60) as r:
                        if r.status_code == 404:
                            logger.warning(f"Audio not found (404): {url}")
                            return False, "not_found"
                        elif r.status_code == 403:
                            logger.warning(f"Access forbidden (403): {url}")
                            return False, "forbidden"
                        elif r.status_code >= 500:
                            if attempt < max_retries - 1:
                                wait_time = 2 ** attempt
                                logger.warning(f"Server error ({r.status_code}), retrying in {wait_time}s...")
                                time.sleep(wait_time)
                                continue
                            logger.error(f"Server error ({r.status_code}) after {max_retries} attempts: {url}")
                            return False, "server_error"
                        
                        r.raise_for_status()
                        
                        # Download the file
                        with open(path, 'wb') as f:
                            for chunk in r.iter_content(chunk_size=8192):
                                f.write(chunk)
                        
                        # Validate size if expected
                        if expected_size:
                            actual_size = path.stat().st_size
                            if actual_size != expected_size:
                                logger.warning(f"Size mismatch: expected {expected_size}, got {actual_size}: {url}")
                                path.unlink()
                                if attempt < max_retries - 1:
                                    continue
                                return False, "size_mismatch"
                        
                        return True, "downloaded"
                        
                except requests.exceptions.Timeout:
                    if attempt < max_retries - 1:
                        wait_time = 2 ** attempt
                        logger.warning(f"Timeout, retrying in {wait_time}s: {url}")
                        time.sleep(wait_time)
                        continue
                    logger.error(f"Timeout after {max_retries} attempts: {url}")
                    return False, "timeout"
                    
                except requests.exceptions.RequestException as e:
                    if attempt < max_retries - 1:
                        wait_time = 2 ** attempt
                        logger.warning(f"Request error ({e}), retrying in {wait_time}s...")
                        time.sleep(wait_time)
                        continue
                    logger.error(f"Request failed after {max_retries} attempts: {url}: {e}")
                    if path.exists():
                        path.unlink()
                    return False, "error"
            
            return False, "max_retries"
            
        except Exception as e:
            logger.error(f"Unexpected error downloading {url}: {e}")
            if path.exists():
                path.unlink()
            return False, "error"

    # ========================================================================
    # STATION METHODS
    # ========================================================================

    def fetch_all_stations(self, ne: Dict, sw: Dict, query: str = None, 
                           period: Dict = None) -> Generator[Dict, None, None]:
        """Fetch all stations within a bounding box with full field coverage.
        
        Args:
            ne (Dict): Northeast corner coordinates (lat, lon).
            sw (Dict): Southwest corner coordinates (lat, lon).
            query (str, optional): Search query string for filtering stations.
            period (Dict, optional): Time period for station metrics.
            
        Yields:
            Dict: A dictionary representing a station node.
        """
        cursor = None
        current_count = 0
        while True:
            vars = {"ne": ne, "sw": sw, "cursor": cursor, "query": query, "period": period}
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

    def fetch_station(self, station_id: str, period: Dict = None, 
                      top_species_limit: int = 20, sensor_history_limit: int = 48,
                      detections_first: int = 50) -> Optional[Dict]:
        """Fetch a single station with detailed data.
        
        Includes probabilities, top species, nested detection counts, and sensor history.
        
        Args:
            station_id (str): The unique identifier of the station.
            period (Dict, optional): Time period for counts and detections.
            top_species_limit (int): Max number of top species to return (default: 20).
            sensor_history_limit (int): Max history readings per sensor type (default: 48).
            detections_first (int): Max number of recent detections to return (default: 50).
            
        Returns:
            Optional[Dict]: Station data dictionary, or None if not found.
        """
        vars = {
            "id": station_id, 
            "period": period, 
            "topSpeciesLimit": top_species_limit,
            "sensorHistoryLimit": sensor_history_limit,
            "detectionsFirst": detections_first
        }
        data = self._execute_query(QUERY_STATION_SINGLE, vars)
        return data.get("data", {}).get("station")

    # ========================================================================
    # DETECTION METHODS
    # ========================================================================

    def fetch_detections(
        self, 
        station_ids: List[str] = None,
        start: datetime = None, 
        end: datetime = None,
        # Score filters (all variants)
        score_gt: float = None,
        score_lt: float = None,
        score_gte: float = None,
        score_lte: float = None,
        # Confidence filters (all variants)
        confidence_gt: float = None,
        confidence_lt: float = None,
        confidence_gte: float = None,
        confidence_lte: float = None,
        # Probability filters (all variants)
        probability_gt: float = None,
        probability_lt: float = None,
        probability_gte: float = None,
        probability_lte: float = None,
        # Other filters
        valid_soundscape: bool = None, 
        recording_modes: List[str] = None,
        eclipse: bool = None,
        time_of_day_gte: int = None, 
        time_of_day_lte: int = None,
        countries: List[str] = None, 
        continents: List[str] = None,
        species_id: str = None,
        species_ids: List[str] = None,
        classifications: List[str] = None,
        station_types: List[str] = None,
        vote: int = None,
        sort_by: str = None,
        unique_stations: bool = None,
        override_station_filters: bool = None,
        ne: Dict = None,
        sw: Dict = None,
        # Legacy aliases for backwards compatibility
        min_score: float = None,
        min_confidence: float = None,
        min_probability: float = None
    ) -> Generator[Dict, None, None]:
        """Fetch detections with comprehensive filtering options.
        
        All score, confidence, and probability filters support four variants:
        
        *   `_gt`: Greater than (exclusive)
        *   `_lt`: Less than (exclusive)
        *   `_gte`: Greater than or equal (inclusive)
        *   `_lte`: Less than or equal (inclusive)
        
        Legacy parameters (`min_score`, `min_confidence`, `min_probability`) map to `_gte` variants.
        
        Args:
            station_ids (List[str], optional): List of station IDs to filter by.
            start (datetime, optional): Start of the time range.
            end (datetime, optional): End of the time range.
            score_gt (float, optional): Score greater than value.
            score_lt (float, optional): Score less than value.
            score_gte (float, optional): Score greater than or equal value.
            score_lte (float, optional): Score less than or equal value.
            confidence_gt (float, optional): Confidence greater than value.
            confidence_lt (float, optional): Confidence less than value.
            confidence_gte (float, optional): Confidence greater than or equal value.
            confidence_lte (float, optional): Confidence less than or equal value.
            probability_gt (float, optional): Probability greater than value.
            probability_lt (float, optional): Probability less than value.
            probability_gte (float, optional): Probability greater than or equal value.
            probability_lte (float, optional): Probability less than or equal value.
            valid_soundscape (bool, optional): Filter for detections with valid audio.
            recording_modes (List[str], optional): Filter by recording mode.
            eclipse (bool, optional): Filter by eclipse status.
            time_of_day_gte (int, optional): Start minute of day (0-1440).
            time_of_day_lte (int, optional): End minute of day (0-1440).
            countries (List[str], optional): Filter by country names.
            continents (List[str], optional): Filter by continent names.
            species_id (str, optional): Filter by single species ID.
            species_ids (List[str], optional): Filter by multiple species IDs.
            classifications (List[str], optional): Filter by classification string.
            station_types (List[str], optional): Filter by station types.
            vote (int, optional): Filter by vote count.
            sort_by (str, optional): Sort order string.
            unique_stations (bool, optional): Return one detection per station.
            override_station_filters (bool, optional): Override station-level filters.
            ne (Dict, optional): Northest corner for spatial filtering.
            sw (Dict, optional): Southwest corner for spatial filtering.
            min_score (float, optional): Alias for score_gte.
            min_confidence (float, optional): Alias for confidence_gte.
            min_probability (float, optional): Alias for probability_gte.
            
        Yields:
            Dict: A dictionary representing a detection.
        """
        cursor = None
        
        # Handle legacy parameters (map to _gte variants)
        if min_score is not None and score_gte is None:
            score_gte = min_score
        if min_confidence is not None and confidence_gte is None:
            confidence_gte = min_confidence
        if min_probability is not None and probability_gte is None:
            probability_gte = min_probability
        
        # Build period dict with correct field names
        period = None
        if start and end:
            period = {"from": start.isoformat(), "to": end.isoformat()}
        
        while True:
            vars = {
                "stationIds": station_ids,
                "period": period,
                "cursor": cursor,
                # Score filters
                "scoreGt": score_gt,
                "scoreLt": score_lt,
                "scoreGte": score_gte,
                "scoreLte": score_lte,
                # Confidence filters
                "confidenceGt": confidence_gt,
                "confidenceLt": confidence_lt,
                "confidenceGte": confidence_gte,
                "confidenceLte": confidence_lte,
                # Probability filters
                "probabilityGt": probability_gt,
                "probabilityLt": probability_lt,
                "probabilityGte": probability_gte,
                "probabilityLte": probability_lte,
                # Other filters
                "validSoundscape": valid_soundscape,
                "recordingModes": recording_modes,
                "eclipse": eclipse,
                "timeOfDayGte": time_of_day_gte,
                "timeOfDayLte": time_of_day_lte,
                "countries": countries,
                "continents": continents,
                "speciesId": species_id,
                "speciesIds": species_ids,
                "classifications": classifications,
                "stationTypes": station_types,
                "vote": vote,
                "sortBy": sort_by,
                "uniqueStations": unique_stations,
                "overrideStationFilters": override_station_filters,
                "ne": ne,
                "sw": sw,
            }
            
            # Remove None values
            vars = {k: v for k, v in vars.items() if v is not None}
            
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

    # ========================================================================
    # SPECIES METHODS
    # ========================================================================

    def fetch_all_species(self, query: str = "", search_locale: str = None) -> Generator[Dict, None, None]:
        """Fetch all species via search with pagination.
        
        Args:
            query (str): Search query string.
            search_locale (str, optional): Locale for search results.
            
        Yields:
            Dict: A dictionary representing a species node.
        """
        cursor = None
        while True:
            vars = {"cursor": cursor, "query": query, "searchLocale": search_locale}
            data = self._execute_query(QUERY_SPECIES_SEARCH, vars)
            
            # FIXED: Use correct path "searchSpecies" not "species"
            connection = data.get("data", {}).get("searchSpecies", {})
            nodes = connection.get("nodes", [])
            page_info = connection.get("pageInfo", {})
            
            for node in nodes:
                yield node
            
            if not page_info.get("hasNextPage"):
                break
            
            cursor = page_info.get("endCursor")
            time.sleep(0.1)

    def fetch_species(self, species_id: str = None, scientific_name: str = None,
                      period: Dict = None, station_types: List[str] = None) -> Optional[Dict]:
        """Fetch a single species with detailed data.
        
        Args:
            species_id (str, optional): The ID of the species to fetch.
            scientific_name (str, optional): The scientific name to search for.
            period (Dict, optional): Time period for metrics.
            station_types (List[str], optional): Filter stats by station types.
            
        Returns:
            Optional[Dict]: Species data dictionary including detection counts and stations, or None.
        """
        vars = {
            "id": species_id, 
            "scientificName": scientific_name,
            "period": period,
            "stationTypes": station_types
        }
        data = self._execute_query(QUERY_SPECIES_SINGLE, vars)
        return data.get("data", {}).get("species")

    def fetch_species_by_ids(self, ids: List[str]) -> Generator[Dict, None, None]:
        """Fetch multiple species by their IDs.
        
        Args:
            ids (List[str]): List of species IDs to fetch.
            
        Yields:
            Dict: Species data dictionary.
        """
        cursor = None
        while True:
            vars = {"ids": ids, "cursor": cursor}
            data = self._execute_query(QUERY_ALL_SPECIES, vars)
            
            connection = data.get("data", {}).get("allSpecies", {})
            nodes = connection.get("nodes", [])
            page_info = connection.get("pageInfo", {})
            
            for node in nodes:
                yield node
            
            if not page_info.get("hasNextPage"):
                break
            
            cursor = page_info.get("endCursor")
            time.sleep(0.1)

    # ========================================================================
    # BIRDNET SIGHTINGS (FIXED)
    # ========================================================================

    def fetch_birdnet_sightings(
        self, 
        ne: Dict, 
        sw: Dict, 
        start: datetime, 
        end: datetime,
        species_id: str = None
    ) -> Generator[Dict, None, None]:
        """Fetch BirdNET mobile app sightings.
        
        Args:
            ne (Dict): Northeast corner coordinates.
            sw (Dict): Southwest corner coordinates.
            start (datetime): Start time for search.
            end (datetime): End time for search.
            species_id (str, optional): Filter by species ID.
            
        Yields:
            Dict: A dictionary representing a BirdNET sighting.
        """
        cursor = None
        
        # FIXED: Use from/to instead of start/end
        period = {"from": start.isoformat(), "to": end.isoformat()}
        
        while True:
            vars = {
                "ne": ne, 
                "sw": sw, 
                "period": period,
                "cursor": cursor,
                "speciesId": species_id
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

    # ========================================================================
    # AGGREGATE / ANALYTICS METHODS
    # ========================================================================

    def fetch_counts(
        self, 
        period: Dict = None, 
        species_id: str = None,
        station_ids: List[str] = None,
        station_types: List[str] = None,
        ne: Dict = None, 
        sw: Dict = None
    ) -> Dict:
        """Fetch aggregate counts for detections, species, and stations.
        
        Args:
            period (Dict, optional): Time period for counts.
            species_id (str, optional): Filter by species ID.
            station_ids (List[str], optional): Filter by station IDs.
            station_types (List[str], optional): Filter by station types.
            ne (Dict, optional): Northeast corner.
            sw (Dict, optional): Southwest corner.
            
        Returns:
            Dict: Dictionary containing count metrics.
        """
        vars = {
            "period": period,
            "speciesId": species_id,
            "stationIds": station_ids,
            "stationTypes": station_types,
            "ne": ne,
            "sw": sw
        }
        vars = {k: v for k, v in vars.items() if v is not None}
        data = self._execute_query(QUERY_COUNTS, vars)
        return data.get("data", {}).get("counts", {})

    def fetch_daily_detection_counts(
        self, 
        period: Dict = None,
        species_ids: List[str] = None,
        station_ids: List[str] = None
    ) -> List[Dict]:
        """Fetch daily aggregated detection counts by species.
        
        Args:
            period (Dict, optional): Time period for counts.
            species_ids (List[str], optional): Filter by species IDs.
            station_ids (List[str], optional): Filter by station IDs.
        
        Returns:
            List[Dict]: List of daily count records.
        """
        vars = {
            "period": period,
            "speciesIds": species_ids,
            "stationIds": station_ids
        }
        vars = {k: v for k, v in vars.items() if v is not None}
        data = self._execute_query(QUERY_DAILY_DETECTION_COUNTS, vars)
        return data.get("data", {}).get("dailyDetectionCounts", [])

    def fetch_time_of_day_detection_counts(
        self,
        period: Dict = None,
        species_id: str = None,
        station_ids: List[str] = None,
        score_gte: float = None,
        confidence_gte: float = None,
        probability_gte: float = None,
        time_of_day_gte: int = None,
        time_of_day_lte: int = None,
        ne: Dict = None,
        sw: Dict = None
    ) -> List[Dict]:
        """Fetch detection counts binned by time of day.
        
        Args:
            period (Dict, optional): Time period.
            species_id (str, optional): Species ID filter.
            station_ids (List[str], optional): Station IDs filter.
            score_gte, confidence_gte, probability_gte (float, optional): Quality filters.
            time_of_day_gte, time_of_day_lte (int, optional): Minute of day range.
            ne, sw (Dict, optional): Bounding box filters.
            
        Returns:
            List[Dict]: List of time-binned count records.
        """
        vars = {
            "period": period,
            "speciesId": species_id,
            "stationIds": station_ids,
            "scoreGte": score_gte,
            "confidenceGte": confidence_gte,
            "probabilityGte": probability_gte,
            "timeOfDayGte": time_of_day_gte,
            "timeOfDayLte": time_of_day_lte,
            "ne": ne,
            "sw": sw
        }
        vars = {k: v for k, v in vars.items() if v is not None}
        data = self._execute_query(QUERY_TIME_OF_DAY_DETECTION_COUNTS, vars)
        return data.get("data", {}).get("timeOfDayDetectionCounts", [])

    def fetch_top_species(
        self,
        limit: int = 50,
        offset: int = None,
        period: Dict = None,
        species_id: str = None,
        station_types: List[str] = None,
        station_ids: List[str] = None,
        ne: Dict = None,
        sw: Dict = None
    ) -> List[Dict]:
        """Fetch top detected species with counts and breakdowns.
        
        Args:
            limit (int): Max number of species to return.
            offset (int, optional): Pagination offset.
            period (Dict, optional): Time period.
            species_id (str, optional): Filter by species ID.
            station_types (List[str], optional): Filter by station types.
            station_ids (List[str], optional): Filter by station IDs.
            ne, sw (Dict, optional): Bounding box filters.
        
        Returns:
            List[Dict]: List of top species records.
        """
        vars = {
            "limit": limit,
            "offset": offset,
            "period": period,
            "speciesId": species_id,
            "stationTypes": station_types,
            "stationIds": station_ids,
            "ne": ne,
            "sw": sw
        }
        vars = {k: v for k, v in vars.items() if v is not None}
        data = self._execute_query(QUERY_TOP_SPECIES, vars)
        return data.get("data", {}).get("topSpecies", [])

    def fetch_top_birdnet_species(
        self,
        limit: int = 50,
        offset: int = None,
        period: Dict = None,
        species_id: str = None,
        ne: Dict = None,
        sw: Dict = None
    ) -> List[Dict]:
        """Fetch top BirdNET-detected species with counts and breakdowns.
        
        Args:
            limit (int): Max number of species to return.
            offset (int, optional): Pagination offset.
            period (Dict, optional): Time period.
            species_id (str, optional): Filter by species ID.
            ne, sw (Dict, optional): Bounding box filters.
        
        Returns:
            List[Dict]: List of top BirdNET species records.
        """
        vars = {
            "limit": limit,
            "offset": offset,
            "period": period,
            "speciesId": species_id,
            "ne": ne,
            "sw": sw
        }
        vars = {k: v for k, v in vars.items() if v is not None}
        data = self._execute_query(QUERY_TOP_BIRDNET_SPECIES, vars)
        return data.get("data", {}).get("topBirdnetSpecies", [])


# --- Real-Time Subscription Client ---

@dataclass
class BirdWeatherSubscription:
    """Real-time WebSocket subscription client for BirdWeather detections.
    
    Uses ActionCable protocol (Rails GraphQL subscriptions) to receive live detection events.
    
    Example:
        .. code-block:: python
        
            def on_detection(detection):
                print(f"New detection: {detection['species']['commonName']}")
            
            sub = BirdWeatherSubscription()
            sub.subscribe(on_detection, station_ids=["12345"], score_gte=0.8)
            # Runs until stopped or error
            sub.stop()
            
    Attributes:
        ws_url (str): WebSocket URL (default: wss://app.birdweather.com/cable).
        on_detection (Callable): Callback for new detections.
        on_error (Callable): Callback for errors.
        on_connect (Callable): Callback for successful connection.
        on_disconnect (Callable): Callback for disconnection.
    """
    
    ws_url: str = "wss://app.birdweather.com/cable"
    on_detection: Optional[Callable[[Dict], None]] = None
    on_error: Optional[Callable[[Exception], None]] = None
    on_connect: Optional[Callable[[], None]] = None
    on_disconnect: Optional[Callable[[], None]] = None
    
    _ws: Any = None
    _running: bool = False
    _thread: Optional[threading.Thread] = None
    _message_queue: Queue = field(default_factory=Queue)
    
    # Subscription GraphQL query (matches API documentation)
    SUBSCRIPTION_QUERY = """
    subscription NewDetection(
      $speciesIds: [ID!],
      $classifications: [String!],
      $stationIds: [ID!],
      $stationTypes: [String!],
      $continents: [String!],
      $countries: [String!],
      $recordingModes: [String!],
      $scoreGt: Float,
      $scoreLt: Float,
      $scoreGte: Float,
      $scoreLte: Float,
      $confidenceGt: Float,
      $confidenceLt: Float,
      $confidenceGte: Float,
      $confidenceLte: Float,
      $probabilityGt: Float,
      $probabilityLt: Float,
      $probabilityGte: Float,
      $probabilityLte: Float,
      $overrideStationFilters: Boolean,
      $timeOfDayGte: Int,
      $timeOfDayLte: Int
    ) {
      newDetection(
        speciesIds: $speciesIds,
        classifications: $classifications,
        stationIds: $stationIds,
        stationTypes: $stationTypes,
        continents: $continents,
        countries: $countries,
        recordingModes: $recordingModes,
        scoreGt: $scoreGt,
        scoreLt: $scoreLt,
        scoreGte: $scoreGte,
        scoreLte: $scoreLte,
        confidenceGt: $confidenceGt,
        confidenceLt: $confidenceLt,
        confidenceGte: $confidenceGte,
        confidenceLte: $confidenceLte,
        probabilityGt: $probabilityGt,
        probabilityLt: $probabilityLt,
        probabilityGte: $probabilityGte,
        probabilityLte: $probabilityLte,
        overrideStationFilters: $overrideStationFilters,
        timeOfDayGte: $timeOfDayGte,
        timeOfDayLte: $timeOfDayLte
      ) {
        detection {
          id
          timestamp
          confidence
          score
          probability
          certainty
          eclipse
          speciesId
          species {
            id
            commonName
            scientificName
            alpha
            ebirdCode
            imageUrl
            thumbnailUrl
          }
          coords { lat lon }
          soundscape {
            id
            url
            duration
            mode
          }
          station {
            id
            name
            type
            country
            state
            timezone
            coords { lat lon }
          }
        }
      }
    }
    """
    
    def subscribe(
        self,
        callback: Callable[[Dict], None],
        # Filter parameters (all optional)
        species_ids: List[str] = None,
        classifications: List[str] = None,
        station_ids: List[str] = None,
        station_types: List[str] = None,
        continents: List[str] = None,
        countries: List[str] = None,
        recording_modes: List[str] = None,
        # Score filters
        score_gt: float = None,
        score_lt: float = None,
        score_gte: float = None,
        score_lte: float = None,
        # Confidence filters
        confidence_gt: float = None,
        confidence_lt: float = None,
        confidence_gte: float = None,
        confidence_lte: float = None,
        # Probability filters
        probability_gt: float = None,
        probability_lt: float = None,
        probability_gte: float = None,
        probability_lte: float = None,
        # Other
        override_station_filters: bool = None,
        time_of_day_gte: int = None,
        time_of_day_lte: int = None,
        blocking: bool = True
    ) -> None:
        """Subscribe to real-time detection events.
        
        Args:
            callback (Callable[[Dict], None]): Function to call for each detection received.
            species_ids (List[str], optional): Filter by species IDs.
            classifications (List[str], optional): Filter by classification.
            station_ids (List[str], optional): Filter by station IDs.
            station_types (List[str], optional): Filter by station types.
            continents (List[str], optional): Filter by continents.
            countries (List[str], optional): Filter by countries.
            recording_modes (List[str], optional): Filter by recording modes.
            score_gt, score_lt, score_gte, score_lte (float, optional): Score filters.
            confidence_gt, confidence_lt, confidence_gte, confidence_lte (float, optional): Confidence filters.
            probability_gt, probability_lt, probability_gte, probability_lte (float, optional): Probability filters.
            override_station_filters (bool, optional): Override station settings.
            time_of_day_gte, time_of_day_lte (int, optional): Time of day filters.
            blocking (bool): If True (default), blocks until stopped. If False, runs in background.
            
        Raises:
            ImportError: If websocket-client is not installed.
        """
        try:
            import websocket
        except ImportError:
            raise ImportError(
                "websocket-client package required for real-time subscriptions. "
                "Install with: pip install websocket-client"
            )
        
        self.on_detection = callback
        
        # Build subscription variables
        variables = {
            "speciesIds": species_ids,
            "classifications": classifications,
            "stationIds": station_ids,
            "stationTypes": station_types,
            "continents": continents,
            "countries": countries,
            "recordingModes": recording_modes,
            "scoreGt": score_gt,
            "scoreLt": score_lt,
            "scoreGte": score_gte,
            "scoreLte": score_lte,
            "confidenceGt": confidence_gt,
            "confidenceLt": confidence_lt,
            "confidenceGte": confidence_gte,
            "confidenceLte": confidence_lte,
            "probabilityGt": probability_gt,
            "probabilityLt": probability_lt,
            "probabilityGte": probability_gte,
            "probabilityLte": probability_lte,
            "overrideStationFilters": override_station_filters,
            "timeOfDayGte": time_of_day_gte,
            "timeOfDayLte": time_of_day_lte,
        }
        # Remove None values
        variables = {k: v for k, v in variables.items() if v is not None}
        
        def on_message(ws, message):
            try:
                data = json.loads(message)
                msg_type = data.get("type")
                
                if msg_type == "welcome":
                    # Connection established, subscribe to channel
                    subscribe_msg = {
                        "command": "subscribe",
                        "identifier": json.dumps({
                            "channel": "GraphqlChannel",
                            "query": self.SUBSCRIPTION_QUERY,
                            "variables": variables
                        })
                    }
                    ws.send(json.dumps(subscribe_msg))
                    logger.info("Sent subscription request")
                    
                elif msg_type == "confirm_subscription":
                    logger.info("Subscription confirmed")
                    if self.on_connect:
                        self.on_connect()
                    
                elif msg_type == "ping":
                    # Keep-alive ping, no action needed
                    pass
                    
                elif data.get("message"):
                    # Detection event received
                    result = data["message"].get("result", {})
                    detection = result.get("data", {}).get("newDetection", {}).get("detection")
                    if detection and self.on_detection:
                        self.on_detection(detection)
                        
            except json.JSONDecodeError as e:
                logger.warning(f"Invalid JSON received: {e}")
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                if self.on_error:
                    self.on_error(e)
        
        def on_error(ws, error):
            logger.error(f"WebSocket error: {error}")
            if self.on_error:
                self.on_error(error)
        
        def on_close(ws, close_status_code, close_msg):
            logger.info(f"WebSocket closed: {close_status_code} - {close_msg}")
            self._running = False
            if self.on_disconnect:
                self.on_disconnect()
        
        def on_open(ws):
            logger.info("WebSocket connection opened")
        
        # Create WebSocket connection
        self._ws = websocket.WebSocketApp(
            self.ws_url,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
            on_open=on_open,
            header={"User-Agent": HEADERS["User-Agent"]}
        )
        
        self._running = True
        
        if blocking:
            self._ws.run_forever()
        else:
            self._thread = threading.Thread(target=self._ws.run_forever, daemon=True)
            self._thread.start()
            logger.info("Subscription started in background thread")
    
    def stop(self) -> None:
        """Stop the subscription and close the WebSocket connection.
        
        This method will terminate the background thread if running in non-blocking mode.
        """
        self._running = False
        if self._ws:
            self._ws.close()
            logger.info("Subscription stopped")
    
    def is_running(self) -> bool:
        """Check if the subscription is currently active.
        
        Returns:
            bool: True if the subscription is running, False otherwise.
        """
        return self._running


# --- Data Ingestor ---

class DataIngestor:
    """Orchestrates the complete BirdWeather data ingestion pipeline.
    
    Attributes:
        output_dir (Path): The root directory for storing ingested data.
        client (BirdWeatherClient): The client used for API requests.
    """

    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.client = BirdWeatherClient()

    def _write_jsonl(self, filepath: Path, records: List[Dict]) -> int:
        """Write records to a JSONL file with ingestion timestamp.
        
        Args:
            filepath (Path): Path to the output JSONL file.
            records (List[Dict]): List of records to write.
            
        Returns:
            int: Number of records written.
        """
        count = 0
        with open(filepath, "w", encoding="utf-8") as f:
            for record in records:
                record["_ingested_at"] = datetime.utcnow().isoformat()
                f.write(json.dumps(record) + "\n")
                count += 1
        return count

    def _append_jsonl(self, filepath: Path, record: Dict) -> None:
        """Append a single record to a JSONL file.
        
        Args:
            filepath (Path): Path to the output JSONL file.
            record (Dict): Record to append.
        """
        record["_ingested_at"] = datetime.utcnow().isoformat()
        with open(filepath, "a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")

    # ========================================================================
    # INGESTION METHODS
    # ========================================================================

    def ingest_species_metadata(self, search_locale: str = None) -> int:
        """Ingest the complete species taxonomy metadata.
        
        Args:
            search_locale (str, optional): Locale for search results.
            
        Returns:
            int: Number of species records ingested.
        """
        logger.info("Ingesting Species Metadata...")
        filename = self.output_dir / "species_metadata.jsonl"
        count = 0
        with open(filename, "w", encoding="utf-8") as f:
            for sp in self.client.fetch_all_species(search_locale=search_locale):
                sp["_ingested_at"] = datetime.utcnow().isoformat()
                f.write(json.dumps(sp) + "\n")
                count += 1
        logger.info(f"Ingested {count} species records.")
        return count

    def ingest_detailed_species(self, species_ids: List[str], period: Dict = None) -> int:
        """Ingest detailed species data including detection counts and station lists.
        
        Args:
            species_ids (List[str]): List of species IDs to fetch.
            period (Dict, optional): Time period for metrics.
            
        Returns:
            int: Number of detailed records ingested.
        """
        logger.info(f"Ingesting detailed data for {len(species_ids)} species...")
        filename = self.output_dir / f"species_detailed_{date.today()}.jsonl"
        count = 0
        with open(filename, "w", encoding="utf-8") as f:
            for sid in species_ids:
                sp = self.client.fetch_species(species_id=sid, period=period)
                if sp:
                    sp["_ingested_at"] = datetime.utcnow().isoformat()
                    f.write(json.dumps(sp) + "\n")
                    count += 1
                time.sleep(0.2)
        logger.info(f"Ingested {count} detailed species records.")
        return count

    def ingest_stations(self, ne: Dict, sw: Dict, query: str = None, 
                        period: Dict = None) -> List[Dict]:
        """Ingest all stations within a bounding box.
        
        Args:
            ne (Dict): Northeast corner.
            sw (Dict): Southwest corner.
            query (str, optional): Search query.
            period (Dict, optional): Time period.
            
        Returns:
            List[Dict]: List of ingested station records.
        """
        logger.info("Ingesting Stations...")
        filename = self.output_dir / f"stations_{date.today()}.jsonl"
        stations = []
        with open(filename, "w", encoding="utf-8") as f:
            for st in self.client.fetch_all_stations(ne, sw, query=query, period=period):
                st["_ingested_at"] = datetime.utcnow().isoformat()
                stations.append(st)
                f.write(json.dumps(st) + "\n")
        logger.info(f"Ingested {len(stations)} stations.")
        return stations

    def ingest_detailed_stations(self, station_ids: List[str], period: Dict = None,
                                  top_species_limit: int = 20) -> int:
        """Ingest detailed station data including probabilities and top species.
        
        Args:
            station_ids (List[str]): List of station IDs to fetch.
            period (Dict, optional): Time period for metrics.
            top_species_limit (int, optional): Max top species.
            
        Returns:
            int: Number of detailed station records ingested.
        """
        logger.info(f"Ingesting detailed data for {len(station_ids)} stations...")
        filename = self.output_dir / f"stations_detailed_{date.today()}.jsonl"
        count = 0
        with open(filename, "w", encoding="utf-8") as f:
            for sid in station_ids:
                st = self.client.fetch_station(sid, period=period, 
                                                top_species_limit=top_species_limit)
                if st:
                    st["_ingested_at"] = datetime.utcnow().isoformat()
                    f.write(json.dumps(st) + "\n")
                    count += 1
                time.sleep(0.2)
        logger.info(f"Ingested {count} detailed station records.")
        return count

    def ingest_detections(
        self,
        station_ids: List[str],
        start: datetime,
        end: datetime,
        download_audio: bool = False,
        **filter_kwargs
    ) -> int:
        """Ingest detections for specified stations with optional audio download.
        
        Args:
            station_ids (List[str]): List of station IDs.
            start (datetime): Start time.
            end (datetime): End time.
            download_audio (bool): Whether to download audio files.
            **filter_kwargs: Additional filters passed to `fetch_detections`.
            
        Returns:
            int: Total number of detections ingested.
        """
        logger.info(f"Ingesting detections from {start.date()} to {end.date()}...")
        
        total_count = 0
        for station_id in station_ids:
            day_count = 0
            curr_d = start
            
            while curr_d < end:
                next_d = curr_d + timedelta(days=1)
                day_str = curr_d.strftime("%Y-%m-%d")
                
                out_file = self.output_dir / f"detections_{station_id}_{day_str}.jsonl"
                
                if out_file.exists() and out_file.stat().st_size > 0:
                    logger.debug(f"Skipping {station_id} {day_str} (already exists)")
                    curr_d = next_d
                    continue
                
                logger.info(f"Fetching detections for station {station_id} on {day_str}")
                
                detections = []
                for det in self.client.fetch_detections(
                    station_ids=[station_id],
                    start=curr_d,
                    end=next_d,
                    **filter_kwargs
                ):
                    det["station_id"] = station_id
                    det["_ingested_at"] = datetime.utcnow().isoformat()
                    detections.append(det)
                
                if detections:
                    with open(out_file, "w", encoding="utf-8") as f:
                        for det in detections:
                            f.write(json.dumps(det) + "\n")
                    
                    logger.info(f"-> Saved {len(detections)} detections")
                    day_count += len(detections)
                    
                    # Optional audio download
                    if download_audio:
                        self._download_detection_audio(station_id, day_str, detections)
                else:
                    out_file.touch()
                    logger.info("-> No detections")
                
                curr_d = next_d
            
            total_count += day_count
        
        logger.info(f"Total detections ingested: {total_count}")
        return total_count

    def _download_detection_audio(
        self, 
        station_id: str, 
        day_str: str, 
        detections: List[Dict],
        show_progress: bool = True
    ) -> DownloadStats:
        """Download audio files for detections with detailed progress tracking.
        
        Args:
            station_id (str): Station ID for directory organization.
            day_str (str): Date string for directory organization.
            detections (List[Dict]): List of detection dictionaries with soundscape data.
            show_progress (bool): Whether to log progress during download (default: True).
            
        Returns:
            DownloadStats: Object containing download counts and error breakdown.
        """
        audio_dir = self.output_dir / "audio" / str(station_id) / day_str
        stats = DownloadStats(total=len(detections))
        
        logger.info(f"  Processing {len(detections)} detections for audio download...")
        
        for i, det in enumerate(detections):
            soundscape = det.get("soundscape")
            
            if not soundscape:
                stats.no_url += 1
                continue
                
            url = soundscape.get("url")
            if not url:
                stats.no_url += 1
                continue
            
            # Use default extension based on URL or fallback to .flac
            filename = soundscape.get("downloadFilename")
            if not filename:
                ext = ".flac" if ".flac" in url else ".mp3"
                filename = f"{det['id']}{ext}"
            
            path = audio_dir / filename
            expected_size = soundscape.get("filesize")
            
            success, status = self.client.download_file(url, path, expected_size)
            
            if success:
                if status == "already_exists":
                    stats.already_exists += 1
                else:
                    stats.downloaded += 1
            else:
                stats.failed += 1
                stats.add_error(status)
            
            # Progress logging every 10 files or at completion
            if show_progress and ((i + 1) % 10 == 0 or i == len(detections) - 1):
                logger.info(
                    f"    Progress: {i + 1}/{len(detections)} | "
                    f"{stats.downloaded} new, {stats.already_exists} cached, {stats.failed} failed"
                )
        
        # Final summary
        logger.info(f"  -> Audio: {stats.summary()}{stats.error_summary()}")
        
        return stats

    def _download_species_images(self, species_list: List[Dict]) -> Dict[str, int]:
        """Download images for all species in the list.
        
        Downloads both full-size images (400x400) and thumbnails (100x100).
        
        Args:
            species_list (List[Dict]): List of species dictionaries with imageUrl/thumbnailUrl.
            
        Returns:
            Dict[str, int]: Counts with keys "images" and "thumbnails".
        """
        images_dir = self.output_dir / "species_images"
        images_dir.mkdir(exist_ok=True)
        
        thumbnails_dir = self.output_dir / "species_thumbnails"
        thumbnails_dir.mkdir(exist_ok=True)
        
        downloaded = {"images": 0, "thumbnails": 0}
        
        for species in species_list:
            species_id = species.get("id")
            if not species_id:
                continue
            
            # Download full image
            image_url = species.get("imageUrl")
            if image_url:
                # Extract extension from URL or default to .jpg
                ext = ".jpg"
                if "." in image_url.split("/")[-1]:
                    ext = "." + image_url.split(".")[-1].split("?")[0]
                
                image_path = images_dir / f"{species_id}{ext}"
                if not image_path.exists():
                    success, _ = self.client.download_file(image_url, image_path)
                    if success:
                        downloaded["images"] += 1
            
            # Download thumbnail
            thumb_url = species.get("thumbnailUrl")
            if thumb_url:
                ext = ".jpg"
                if "." in thumb_url.split("/")[-1]:
                    ext = "." + thumb_url.split(".")[-1].split("?")[0]
                
                thumb_path = thumbnails_dir / f"{species_id}_thumb{ext}"
                if not thumb_path.exists():
                    success, _ = self.client.download_file(thumb_url, thumb_path)
                    if success:
                        downloaded["thumbnails"] += 1
        
        if downloaded["images"] or downloaded["thumbnails"]:
            logger.info(f"-> Downloaded {downloaded['images']} images, {downloaded['thumbnails']} thumbnails")
        
        return downloaded

    def _download_station_media(self, stations: List[Dict]) -> Dict[str, int]:
        """Download audio and video files for stations.

        Downloads audioUrl and videoUrl from station records.
        
        Args:
            stations (List[Dict]): List of station dictionaries with audioUrl/videoUrl.
            
        Returns:
            Dict[str, int]: Counts with keys "audio" and "video".
        """
        audio_dir = self.output_dir / "station_media" / "audio"
        audio_dir.mkdir(parents=True, exist_ok=True)
        
        video_dir = self.output_dir / "station_media" / "video"
        video_dir.mkdir(parents=True, exist_ok=True)
        
        downloaded = {"audio": 0, "video": 0}
        
        for station in stations:
            station_id = station.get("id")
            if not station_id:
                continue
            
            # Download audio
            audio_url = station.get("audioUrl")
            if audio_url:
                ext = ".mp3"
                if "." in audio_url.split("/")[-1]:
                    ext = "." + audio_url.split(".")[-1].split("?")[0]
                
                audio_path = audio_dir / f"{station_id}{ext}"
                if not audio_path.exists():
                    success, _ = self.client.download_file(audio_url, audio_path)
                    if success:
                        downloaded["audio"] += 1
            
            # Download video
            video_url = station.get("videoUrl")
            if video_url:
                ext = ".mp4"
                if "." in video_url.split("/")[-1]:
                    ext = "." + video_url.split(".")[-1].split("?")[0]
                
                video_path = video_dir / f"{station_id}{ext}"
                if not video_path.exists():
                    success, _ = self.client.download_file(video_url, video_path)
                    if success:
                        downloaded["video"] += 1
        
        if downloaded["audio"] or downloaded["video"]:
            logger.info(f"-> Downloaded {downloaded['audio']} station audio, {downloaded['video']} station video files")
        
        return downloaded


    def ingest_birdnet_sightings(self, ne: Dict, sw: Dict, start: datetime, 
                                  end: datetime) -> int:
        """Ingest BirdNET mobile app sightings.
        
        Args:
            ne (Dict): Northeast corner.
            sw (Dict): Southwest corner.
            start (datetime): Start time.
            end (datetime): End time.
            
        Returns:
            int: Number of sightings ingested.
        """
        logger.info("Ingesting BirdNET Sightings...")
        filename = self.output_dir / f"birdnet_sightings_{date.today()}.jsonl"
        count = 0
        
        with open(filename, "w", encoding="utf-8") as f:
            for sighting in self.client.fetch_birdnet_sightings(ne, sw, start, end):
                sighting["_ingested_at"] = datetime.utcnow().isoformat()
                f.write(json.dumps(sighting) + "\n")
                count += 1
        
        logger.info(f"Ingested {count} BirdNET sightings.")
        return count

    def ingest_detections_by_species(
        self, 
        species_ids: List[str],
        ne: Dict, 
        sw: Dict, 
        start: datetime, 
        end: datetime,
        download_audio: bool = False,
        min_score: float = None,
        min_confidence: float = None
    ) -> Dict[str, int]:
        """Ingest detections by species ID (bypasses stations endpoint).
        
        This is a workaround for when the stations endpoint is failing.
        Queries detections directly filtered by species.
        
        Args:
            species_ids (List[str]): List of species IDs to fetch detections for.
            ne (Dict): Northeast corner.
            sw (Dict): Southwest corner.
            start (datetime): Start time.
            end (datetime): End time.
            download_audio (bool): Whether to download audio files.
            min_score (float, optional): Minimum detection score filter.
            min_confidence (float, optional): Minimum confidence filter.
            
        Returns:
            Dict[str, int]: Dictionary with detection and audio counts per species.
        """
        logger.info(f"Ingesting detections for {len(species_ids)} species...")
        
        results = {}
        total_detections = 0
        total_audio = 0
        
        period = {"from": start.isoformat(), "to": end.isoformat()}
        
        for species_id in species_ids:
            logger.info(f"Fetching detections for species {species_id}...")
            
            species_detections = []
            
            # Fetch detections for this species
            for det in self.client.fetch_detections(
                species_ids=[species_id],
                start=start,
                end=end,
                ne=ne,
                sw=sw,
                min_score=min_score,
                min_confidence=min_confidence
            ):
                det["_ingested_at"] = datetime.utcnow().isoformat()
                species_detections.append(det)
            
            if species_detections:
                # Get species name for filename
                species_name = species_detections[0].get("species", {}).get("commonName", species_id)
                species_name_safe = species_name.replace(" ", "_").replace("/", "-")
                
                # Save detections
                out_file = self.output_dir / f"detections_species_{species_name_safe}_{date.today()}.jsonl"
                with open(out_file, "w", encoding="utf-8") as f:
                    for det in species_detections:
                        f.write(json.dumps(det) + "\n")
                
                logger.info(f"  -> Saved {len(species_detections)} detections for {species_name}")
                total_detections += len(species_detections)
                
                # Download audio if requested
                audio_count = 0
                if download_audio:
                    audio_dir = self.output_dir / "audio" / species_name_safe
                    audio_dir.mkdir(parents=True, exist_ok=True)
                    
                    stats = DownloadStats(total=len(species_detections))
                    logger.info(f"  Processing {len(species_detections)} detections for audio download...")
                    
                    for i, det in enumerate(species_detections):
                        soundscape = det.get("soundscape")
                        if not soundscape or not soundscape.get("url"):
                            stats.no_url += 1
                            continue
                        
                        audio_url = soundscape["url"]
                        detection_id = det.get("id", "unknown")
                        
                        # Determine file extension from URL
                        ext = ".flac" if ".flac" in audio_url else (".wav" if ".wav" in audio_url else ".mp3")
                        filename = soundscape.get("downloadFilename", f"{detection_id}{ext}")
                        audio_path = audio_dir / filename
                        expected_size = soundscape.get("filesize")
                        
                        success, status = self.client.download_file(audio_url, audio_path, expected_size)
                        
                        if success:
                            if status == "already_exists":
                                stats.already_exists += 1
                            else:
                                stats.downloaded += 1
                        else:
                            stats.failed += 1
                            stats.add_error(status)
                        
                        # Progress logging every 10 files
                        if (i + 1) % 10 == 0 or i == len(species_detections) - 1:
                            logger.info(
                                f"    Progress: {i + 1}/{len(species_detections)} | "
                                f"{stats.downloaded} new, {stats.already_exists} cached, {stats.failed} failed"
                            )
                    
                    audio_count = stats.downloaded
                    logger.info(f"  -> Audio: {stats.summary()}{stats.error_summary()}")
                    total_audio += stats.downloaded
                
                results[species_id] = {
                    "species_name": species_name,
                    "detections": len(species_detections),
                    "audio_files": audio_count
                }
        
        logger.info(f"Total: {total_detections} detections, {total_audio} audio files")
        return results

    def ingest_aggregates(self, period: Dict, ne: Dict = None, sw: Dict = None) -> None:
        """Ingest all aggregate statistics.
        
        Args:
            period (Dict): Time period for aggregates.
            ne (Dict, optional): Northeast corner.
            sw (Dict, optional): Southwest corner.
        """
        logger.info("Ingesting Aggregate Data...")
        
        # Global counts
        counts = self.client.fetch_counts(period=period, ne=ne, sw=sw)
        counts["_ingested_at"] = datetime.utcnow().isoformat()
        counts["period"] = period
        
        with open(self.output_dir / f"counts_{date.today()}.json", "w") as f:
            json.dump(counts, f, indent=2)
        
        # Daily detection counts
        daily = self.client.fetch_daily_detection_counts(period=period)
        self._write_jsonl(self.output_dir / f"daily_counts_{date.today()}.jsonl", daily)
        
        # Time of day counts
        tod = self.client.fetch_time_of_day_detection_counts(period=period, ne=ne, sw=sw)
        self._write_jsonl(self.output_dir / f"time_of_day_counts_{date.today()}.jsonl", tod)
        
        # Top species
        top_sp = self.client.fetch_top_species(limit=100, period=period, ne=ne, sw=sw)
        self._write_jsonl(self.output_dir / f"top_species_{date.today()}.jsonl", top_sp)
        
        # Top BirdNET species
        top_bn = self.client.fetch_top_birdnet_species(limit=100, period=period, ne=ne, sw=sw)
        self._write_jsonl(self.output_dir / f"top_birdnet_species_{date.today()}.jsonl", top_bn)
        
        logger.info("Finished Aggregate Data.")

    def run(
        self,
        ne: Dict = DEFAULT_NE,
        sw: Dict = DEFAULT_SW,
        days: int = 7,
        start_date: str = None,
        end_date: str = None,
        min_score: float = None,
        min_confidence: float = None,
        min_probability: float = None,
        download_audio: bool = False,
        download_species_images: bool = False,
        download_station_media: bool = False,
        ingest_species: bool = False,
        ingest_birdnet: bool = False,
        ingest_aggregates: bool = False,
        ingest_detailed_stations: bool = False,
        ingest_detailed_species: bool = False,
        skip_stations: bool = False,
        station_query: str = None,
        valid_soundscape: bool = None,
        recording_modes: List[str] = None,
        eclipse: bool = None,
        time_of_day_gte: int = None,
        time_of_day_lte: int = None,
        countries: List[str] = None,
        continents: List[str] = None,
        station_types: List[str] = None,
        species_ids: List[str] = None,
        vote: int = None,
        sort_by: str = None,
        unique_stations: bool = None
    ) -> None:
        """Execute the complete data ingestion pipeline.
        
        Args:
            ne (Dict): Northeast corner (default: Netherlands).
            sw (Dict): Southwest corner (default: Netherlands).
            days (int): Number of days to look back (default: 7).
            start_date (str, optional): Start date (YYYY-MM-DD).
            end_date (str, optional): End date (YYYY-MM-DD).
            min_score (float, optional): Minimum score filter.
            min_confidence (float, optional): Minimum confidence filter.
            min_probability (float, optional): Minimum probability filter.
            download_audio (bool): If True, download detection audio.
            download_species_images (bool): If True, download species images.
            download_station_media (bool): If True, download station audio/video.
            ingest_species (bool): If True, ingest species metadata.
            ingest_birdnet (bool): If True, ingest BirdNET sightings.
            ingest_aggregates (bool): If True, ingest aggregate stats.
            ingest_detailed_stations (bool): If True, ingest detailed station data.
            ingest_detailed_species (bool): If True, ingest detailed species data.
            skip_stations (bool): If True, skip station ingestion (load from file).
            station_query (str, optional): Station search query.
            valid_soundscape (bool, optional): Filter for valid soundscape.
            recording_modes (List[str], optional): Filter by recording modes.
            eclipse (bool, optional): Filter by eclipse status.
            time_of_day_gte (int, optional): Minute of day start.
            time_of_day_lte (int, optional): Minute of day end.
            countries (List[str], optional): Filter by countries.
            continents (List[str], optional): Filter by continents.
            station_types (List[str], optional): Filter by station types.
            species_ids (List[str], optional): Filter by species IDs.
            vote (int, optional): Filter by vote.
            sort_by (str, optional): Sort order.
            unique_stations (bool, optional): Unique stations only.
        """
        
        # Calculate date range
        if start_date and end_date:
            start = datetime.strptime(start_date, "%Y-%m-%d")
            end = datetime.strptime(end_date, "%Y-%m-%d")
        else:
            end = datetime.now()
            start = end - timedelta(days=days)
        
        period = {"from": start.isoformat(), "to": end.isoformat()}
        
        # 1. Species metadata
        if ingest_species:
            self.ingest_species_metadata()
        
        # 2. Aggregates
        if ingest_aggregates:
            self.ingest_aggregates(period, ne, sw)
        
        # 3. BirdNET sightings
        if ingest_birdnet:
            self.ingest_birdnet_sightings(ne, sw, start, end)
        
        # 4. Stations
        stations = []
        if not skip_stations:
            stations = self.ingest_stations(ne, sw, query=station_query, period=period)
        else:
            stat_file = self.output_dir / f"stations_{date.today()}.jsonl"
            if stat_file.exists():
                with open(stat_file, "r") as f:
                    stations = [json.loads(line) for line in f]
                logger.info(f"Loaded {len(stations)} stations from file")
            else:
                logger.error("Cannot skip: no station file found")
                return
        
        # 5. Detailed stations (optional)
        if ingest_detailed_stations and stations:
            station_ids = [s["id"] for s in stations]
            self.ingest_detailed_stations(station_ids, period=period)
        
        # 6. Detailed species (optional)
        if ingest_detailed_species and species_ids:
            self.ingest_detailed_species(species_ids, period=period)
        
        # 7. Detections
        if stations:
            station_ids = [s["id"] for s in stations]
            self.ingest_detections(
                station_ids=station_ids,
                start=start,
                end=end,
                download_audio=download_audio,
                min_score=min_score,
                min_confidence=min_confidence,
                min_probability=min_probability,
                valid_soundscape=valid_soundscape,
                recording_modes=recording_modes,
                eclipse=eclipse,
                time_of_day_gte=time_of_day_gte,
                time_of_day_lte=time_of_day_lte,
                countries=countries,
                continents=continents,
                station_types=station_types,
                species_ids=species_ids,
                vote=vote,
                sort_by=sort_by,
                unique_stations=unique_stations
            )
        
        # 8. Download species images (optional)
        if download_species_images:
            logger.info("Downloading species images...")
            # Load species from file if we ingested them
            species_file = self.output_dir / "species_metadata.jsonl"
            if species_file.exists():
                species_list = []
                with open(species_file, "r") as f:
                    for line in f:
                        species_list.append(json.loads(line))
                self._download_species_images(species_list)
            else:
                logger.warning("No species file found. Run with --ingest-species first.")
        
        # 9. Download station media (optional)
        if download_station_media and stations:
            logger.info("Downloading station media (audio/video)...")
            self._download_station_media(stations)


@dataclass
class CollectionStats:
    """Statistics for a collection run.
    
    Attributes:
        start_time (datetime): When the collection started.
        end_time (Optional[datetime]): When the collection ended.
        species_queried (int): Number of species queried.
        detections_found (int): Total detections found.
        detections_with_audio (int): Detections that have audio URLs.
        audio_downloaded (int): Number of audio files downloaded.
        audio_cached (int): Number of audio files skipped (cached).
        audio_failed (int): Number of audio downloads failed.
        errors (List[str]): List of error messages.
    """
    start_time: datetime = field(default_factory=datetime.now)
    end_time: Optional[datetime] = None
    species_queried: int = 0
    detections_found: int = 0
    detections_with_audio: int = 0
    audio_downloaded: int = 0
    audio_cached: int = 0
    audio_failed: int = 0
    errors: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict:
        """Convert stats to dictionary for serialization.
        
        Returns:
            Dict: Dictionary representation of stats.
        """
        return {
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_seconds": (self.end_time - self.start_time).total_seconds() if self.end_time else None,
            "species_queried": self.species_queried,
            "detections_found": self.detections_found,
            "detections_with_audio": self.detections_with_audio,
            "audio_downloaded": self.audio_downloaded,
            "audio_cached": self.audio_cached,
            "audio_failed": self.audio_failed,
            "errors": self.errors
        }


class AudioCollector:
    """Automated audio collection that bypasses the stations endpoint.
    
    Attributes:
        output_dir (Path): Root directory for output.
        client (BirdWeatherClient): API client.
    """
    
    def __init__(self, output_dir: Path = Path("data/raw/birdweather")):
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.client = BirdWeatherClient()
        self._species_cache: Optional[List[Dict]] = None
    
    def get_species_list(self, force_refresh: bool = False) -> List[Dict]:
        """Get the full species list, using cache if available.
        
        Args:
            force_refresh (bool): If True, ignore cache and fetch fresh list.
            
        Returns:
            List[Dict]: List of species dictionaries with id, commonName, scientificName.
        """
        cache_file = self.output_dir / "species_cache.json"
        
        # Try to load from cache
        if not force_refresh and cache_file.exists():
            cache_age = datetime.now().timestamp() - cache_file.stat().st_mtime
            if cache_age < 86400:  # Cache valid for 24 hours
                logger.info("Loading species from cache...")
                with open(cache_file, "r") as f:
                    self._species_cache = json.load(f)
                logger.info(f"Loaded {len(self._species_cache)} species from cache")
                return self._species_cache
        
        # Fetch fresh species list
        logger.info("Fetching species list from API...")
        species_list = []
        for species in self.client.fetch_all_species():
            species_list.append({
                "id": species["id"],
                "commonName": species.get("commonName", "Unknown"),
                "scientificName": species.get("scientificName", "")
            })
        
        # Cache for future use
        with open(cache_file, "w") as f:
            json.dump(species_list, f, indent=2)
        
        logger.info(f"Fetched and cached {len(species_list)} species")
        self._species_cache = species_list
        return species_list
    
    def _download_single_audio(self, task: Dict) -> Tuple[bool, str]:
        """Download a single audio file (for parallel processing).
        
        Args:
            task (Dict): Dictionary containing 'url', 'audio_path', and optional 'expected_size'.
            
        Returns:
            Tuple[bool, str]: Tuple of (success, status).
        """
        url = task["url"]
        audio_path = Path(task["audio_path"])
        expected_size = task.get("expected_size")
        
        # Skip if already exists
        if audio_path.exists():
            if expected_size and audio_path.stat().st_size == expected_size:
                return True, "already_exists"
        
        return self.client.download_file(url, audio_path, expected_size)
    
    def collect(
        self,
        hours: Optional[int] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        min_confidence: float = 0.9,
        continent: Optional[str] = None,
        country: Optional[str] = None,
        species_ids: Optional[List[str]] = None,
        download_audio: bool = True,
        max_detections: Optional[int] = None,
        workers: int = 10
    ) -> CollectionStats:
        """Collect all detections and audio.
        
        This method fetches detections directly (without per-species queries),
        which is more efficient and bypasses the failing stations endpoint.
        
        Args:
            hours (Optional[int]): Number of hours to look back (ignored if start_time/end_time set).
            start_time (Optional[datetime]): Start of time range.
            end_time (Optional[datetime]): End of time range.
            min_confidence (float): Minimum detection confidence (default: 0.9).
            continent (Optional[str]): Continent filter (e.g., "Europe").
            country (Optional[str]): Country filter (e.g., "Netherlands").
            species_ids (Optional[List[str]]): List of species IDs to filter by.
            download_audio (bool): If True, download audio files.
            max_detections (Optional[int]): Limit number of detections (for testing).
            workers (int): Number of parallel download workers (default: 10).
            
        Returns:
            CollectionStats: Summary statistics of what was collected.
        """
        stats = CollectionStats()
        
        # Calculate time range
        if start_time and end_time:
            pass  # Use provided times
        elif hours:
            end_time = datetime.now()
            start_time = end_time - timedelta(hours=hours)
        else:
            hours = 24  # Default
            end_time = datetime.now()
            start_time = end_time - timedelta(hours=hours)
        
        today = end_time.strftime("%Y-%m-%d")
        
        logger.info(f"Collecting data from {start_time} to {end_time}")
        logger.info(f"Output directory: {self.output_dir / today}")
        
        # Create date-based output directory
        date_dir = self.output_dir / today
        date_dir.mkdir(exist_ok=True)
        audio_dir = date_dir / "audio"
        audio_dir.mkdir(exist_ok=True)
        
        # Build filters
        continents = [continent] if continent else None
        countries = [country] if country else None
        
        filter_desc = []
        if continent:
            filter_desc.append(f"continent={continent}")
        if country:
            filter_desc.append(f"country={country}")
        if species_ids:
            filter_desc.append(f"species={','.join(species_ids)}")
        filter_str = ", ".join(filter_desc) if filter_desc else "no filter"
        
        logger.info(f"Fetching detections ({filter_str})...")
        
        all_detections = []
        species_seen: Dict[str, str] = {}  # id -> name
        
        for det in self.client.fetch_detections(
            start=start_time,
            end=end_time,
            species_ids=species_ids,
            continents=continents,
            countries=countries,
            confidence_gte=min_confidence,
            valid_soundscape=True if download_audio else None
        ):
            det["_collected_at"] = datetime.now().isoformat()
            
            # Track species
            species_id = det.get("speciesId", det.get("species", {}).get("id"))
            species_name = det.get("species", {}).get("commonName", "Unknown")
            if species_id:
                species_seen[str(species_id)] = species_name
            
            all_detections.append(det)
            stats.detections_found += 1
            
            # Count those with audio
            if det.get("soundscape", {}).get("url"):
                stats.detections_with_audio += 1
            
            # Progress logging
            if stats.detections_found % 100 == 0:
                logger.info(f"  Fetched {stats.detections_found} detections...")
            
            # Limit for testing
            if max_detections and stats.detections_found >= max_detections:
                logger.info(f"  Reached limit of {max_detections} detections")
                break
        
        logger.info(f"\nTotal: {stats.detections_found} detections from {len(species_seen)} species")
        stats.species_queried = len(species_seen)
        
        # Save detections
        detections_file = date_dir / "detections.jsonl"
        with open(detections_file, "w", encoding="utf-8") as f:
            for det in all_detections:
                f.write(json.dumps(det) + "\n")
        logger.info(f"Saved detections to {detections_file}")
        
        # Save species list for this collection
        species_file = date_dir / "species_list.json"
        species_data = [
            {"id": k, "commonName": v}
            for k, v in species_seen.items()
        ]
        with open(species_file, "w", encoding="utf-8") as f:
            json.dump(species_data, f, indent=2)
        logger.info(f"Saved {len(species_data)} species to {species_file}")
        
        # Download audio with parallel workers
        if download_audio and stats.detections_with_audio > 0:
            logger.info(f"\nDownloading audio for {stats.detections_with_audio} detections with {workers} parallel workers...")
            
            # Prepare download tasks
            download_tasks = []
            for det in all_detections:
                soundscape = det.get("soundscape")
                if not soundscape or not soundscape.get("url"):
                    continue
                
                url = soundscape["url"]
                detection_id = det.get("id", "unknown")
                species_id = det.get("speciesId", det.get("species", {}).get("id", "unknown"))
                ext = ".flac" if ".flac" in url else (".wav" if ".wav" in url else ".mp3")
                filename = f"{detection_id}_{species_id}{ext}"
                
                download_tasks.append({
                    "url": url,
                    "audio_path": str(audio_dir / filename),
                    "expected_size": soundscape.get("filesize")
                })
            
            # Execute parallel downloads
            completed = 0
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {executor.submit(self._download_single_audio, task): task for task in download_tasks}
                
                for future in as_completed(futures):
                    try:
                        success, status = future.result()
                        if success:
                            if status == "already_exists":
                                stats.audio_cached += 1
                            else:
                                stats.audio_downloaded += 1
                        else:
                            stats.audio_failed += 1
                    except Exception as e:
                        stats.audio_failed += 1
                        logger.debug(f"Download failed: {e}")
                    
                    completed += 1
                    if completed % 100 == 0 or completed == len(download_tasks):
                        logger.info(
                            f"    Audio progress: {completed}/{len(download_tasks)} | "
                            f"{stats.audio_downloaded} new, {stats.audio_cached} cached, {stats.audio_failed} failed"
                        )
            
            logger.info(
                f"Audio complete: {stats.audio_downloaded} downloaded, "
                f"{stats.audio_cached} cached, {stats.audio_failed} failed"
            )
        
        # Save metadata
        stats.end_time = datetime.now()
        metadata = {
            "collection_time": datetime.now().isoformat(),
            "time_range": {
                "start": start_time.isoformat(),
                "end": end_time.isoformat(),
                "hours": hours
            },
            "filters": {
                "min_confidence": min_confidence,
                "continent": continent,
                "country": country,
                "download_audio": download_audio
            },
            "stats": stats.to_dict()
        }
        
        metadata_file = date_dir / "metadata.json"
        with open(metadata_file, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2)
        logger.info(f"Saved metadata to {metadata_file}")
        
        # Final summary
        logger.info("\n" + "=" * 60)
        logger.info("COLLECTION COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Duration: {(stats.end_time - stats.start_time).total_seconds():.1f} seconds")
        logger.info(f"Species found: {len(species_seen)}")
        logger.info(f"Detections found: {stats.detections_found}")
        logger.info(f"Audio files: {stats.audio_downloaded} new, {stats.audio_cached} cached, {stats.audio_failed} failed")
        logger.info(f"Output: {date_dir}")
        
        return stats


# --- CLI ---

def main() -> None:
    """CLI entrypoint for BirdWeather data ingestion.
    
    By default, uses AudioCollector which bypasses the failing stations endpoint.
    
    Parses command line arguments and invokes the appropriate collector.
    """
    parser = argparse.ArgumentParser(
        description="BirdWeather Data Collection (Automated Pipeline)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Collect last 24 hours with audio (DEFAULT - bypasses stations endpoint)
  python -m avian_biosurveillance.ingestion.birdweather --hours 24
  
  # Collect last 48 hours from Europe only
  python -m avian_biosurveillance.ingestion.birdweather --hours 48 --continent Europe
  
  # Collect from a specific country
  python -m avian_biosurveillance.ingestion.birdweather --hours 24 --country Netherlands
  
  # Collect specific date range
  python -m avian_biosurveillance.ingestion.birdweather --start-date 2026-01-01 --end-date 2026-01-15
  
  # Faster downloads with more workers
  python -m avian_biosurveillance.ingestion.birdweather --hours 24 --workers 20
  
  # Collect without audio download
  python -m avian_biosurveillance.ingestion.birdweather --hours 24 --no-audio
  
  # Use legacy mode (requires stations - may fail with 500 errors)
  python -m avian_biosurveillance.ingestion.birdweather --legacy --days 7

Data Structure:
  data/raw/birdweather/
  └── 2026-01-30/              # Date-based folder
      ├── detections.jsonl     # All detections
      ├── species_list.json    # Species observed  
      ├── metadata.json        # Collection stats
      └── audio/
          └── {detection_id}_{species_id}.flac
        """
    )
    
    # Time range options
    parser.add_argument("--hours", type=int,
                        help="Number of hours to look back (default: 24, ignored if --start-date set)")
    parser.add_argument("--start-date", type=str,
                        help="Start date (YYYY-MM-DD), use with --end-date")
    parser.add_argument("--end-date", type=str,
                        help="End date (YYYY-MM-DD), use with --start-date")
    
    # Geo filters
    parser.add_argument("--continent", type=str,
                        help="Filter by continent (e.g., Europe, NorthAmerica)")
    parser.add_argument("--country", type=str,
                        help="Filter by country (e.g., Netherlands, Germany)")
    parser.add_argument("--species-id", type=str, action="append",
                        help="Filter by species ID (can use multiple times, e.g., --species-id 11 --species-id 6)")
    
    # Quality and output options
    parser.add_argument("--min-confidence", type=float, default=0.9,
                        help="Minimum detection confidence (default: 0.9)")
    parser.add_argument("--output-dir", type=str, default="data/raw/birdweather",
                        help="Output directory")
    parser.add_argument("--no-audio", action="store_true",
                        help="Skip audio download, only collect detections")
    parser.add_argument("--max-detections", type=int,
                        help="Limit number of detections (for testing)")
    parser.add_argument("--workers", type=int, default=10,
                        help="Number of parallel download workers (default: 10)")
    
    # Legacy mode (uses stations endpoint - may fail)
    parser.add_argument("--legacy", action="store_true",
                        help="Use legacy DataIngestor mode (requires stations endpoint)")
    
    # Legacy mode options (only used with --legacy)
    legacy_group = parser.add_argument_group("Legacy mode options (use with --legacy)")
    legacy_group.add_argument("--days", type=int, default=7,
                              help="Number of days to look back")
    legacy_group.add_argument("--ingest-species", action="store_true")
    legacy_group.add_argument("--ingest-aggregates", action="store_true")
    legacy_group.add_argument("--ingest-birdnet", action="store_true")
    legacy_group.add_argument("--download-all-media", action="store_true")
    legacy_group.add_argument("--skip-stations", action="store_true")
    
    args = parser.parse_args()
    
    # DEFAULT MODE: Use AudioCollector (bypasses stations endpoint)
    if not args.legacy:
        logger.info("=" * 60)
        logger.info("BIRDWEATHER AUTOMATED COLLECTION")
        logger.info("=" * 60)
        logger.info("Using direct detection fetching (bypasses stations endpoint)")
        
        # Parse date range if provided
        start_time = None
        end_time = None
        if args.start_date and args.end_date:
            start_time = datetime.strptime(args.start_date, "%Y-%m-%d")
            end_time = datetime.strptime(args.end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
            logger.info(f"Date range: {args.start_date} to {args.end_date}")
        
        collector = AudioCollector(Path(args.output_dir))
        collector.collect(
            hours=args.hours if not (args.start_date and args.end_date) else None,
            start_time=start_time,
            end_time=end_time,
            min_confidence=args.min_confidence,
            continent=args.continent,
            country=args.country,
            species_ids=args.species_id,
            download_audio=not args.no_audio,
            max_detections=args.max_detections,
            workers=args.workers
        )
        return
    
    # LEGACY MODE: Use DataIngestor (requires stations endpoint)
    logger.info("Using LEGACY mode with DataIngestor")
    logger.warning("Note: This may fail if stations endpoint returns 500 errors")
    
    ingestor = DataIngestor(Path(args.output_dir))
    
    download_audio = args.download_all_media
    download_species_images = args.download_all_media
    download_station_media = args.download_all_media
    
    ingestor.run(
        days=args.days,
        download_audio=download_audio,
        download_species_images=download_species_images,
        download_station_media=download_station_media,
        ingest_species=args.ingest_species,
        ingest_birdnet=args.ingest_birdnet,
        ingest_aggregates=args.ingest_aggregates,
        skip_stations=args.skip_stations,
    )


if __name__ == "__main__":
    main()