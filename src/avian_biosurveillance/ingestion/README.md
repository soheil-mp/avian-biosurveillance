# BirdWeather Data Ingestion

Automated pipeline for collecting bird detection data from the [BirdWeather API](https://app.birdweather.com/graphql).

## Quick Start

```bash
# Collect last 24 hours with audio
poetry run python -m avian_biosurveillance.ingestion.birdweather --hours 24

# Collect from Europe with 20 parallel workers (faster)
poetry run python -m avian_biosurveillance.ingestion.birdweather --hours 48 --continent Europe --workers 20

# Collect from a specific country
poetry run python -m avian_biosurveillance.ingestion.birdweather --hours 24 --country Netherlands

# Collect specific bird species
poetry run python -m avian_biosurveillance.ingestion.birdweather --hours 24 --species-id 11 --species-id 6

# Collect specific date range
poetry run python -m avian_biosurveillance.ingestion.birdweather --start-date 2026-01-01 --end-date 2026-01-15
```

## Data Structure

```
data/raw/birdweather/
└── 2026-01-30/              # Date-based folder
    ├── detections.jsonl     # All detections
    ├── species_list.json    # Species observed  
    ├── metadata.json        # Collection stats
    └── audio/
        └── {detection_id}_{species_id}.flac
```

## CLI Options

| Option | Description | Default |
|--------|-------------|---------|
| `--hours N` | Hours to look back | 24 |
| `--start-date` | Start date (YYYY-MM-DD) | - |
| `--end-date` | End date (YYYY-MM-DD) | - |
| `--continent` | Filter (Europe, NorthAmerica, etc.) | - |
| `--country` | Filter (Netherlands, Germany, etc.) | - |
| `--species-id` | Filter by species ID (repeatable) | - |
| `--min-confidence` | Min detection confidence | 0.9 |
| `--workers` | Parallel download workers | 10 |
| `--no-audio` | Skip audio download | False |

## Python API

```python
from avian_biosurveillance.ingestion.birdweather import AudioCollector

collector = AudioCollector()

# By hours with filters
stats = collector.collect(
    hours=24, 
    continent="Europe",
    species_ids=["11", "6"],  # European Robin, House Sparrow
    workers=20
)
```

## Help

```bash
poetry run python -m avian_biosurveillance.ingestion.birdweather --help
```
