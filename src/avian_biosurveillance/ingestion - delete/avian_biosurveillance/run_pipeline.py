#!/usr/bin/env python3
"""
Avian Biosurveillance Pipeline
==============================
Main orchestration script for running data ingestion pipelines.

Usage:
    python run_pipeline.py --help
    python run_pipeline.py ingest --source birdweather
    python run_pipeline.py ingest --all
    python run_pipeline.py status
"""

import argparse
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional
import json

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from config.settings import PipelineConfig, create_default_config
from ingestion import (
    create_birdweather_module,
    create_xenocanto_module,
    create_knmi_module,
    create_mortality_module,
    IngestionMetadata,
    IngestionStatus
)


def setup_logging(level: str = "INFO"):
    """Configure logging for the pipeline."""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('pipeline.log')
        ]
    )


def run_birdweather_ingestion(
    config: PipelineConfig,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None
) -> IngestionMetadata:
    """Run BirdWeather data ingestion."""
    logger = logging.getLogger("pipeline.birdweather")
    
    if not config.birdweather.enabled:
        logger.info("BirdWeather ingestion is disabled")
        return IngestionMetadata(
            source_id="birdweather",
            batch_id="disabled",
            started_at=datetime.utcnow(),
            status=IngestionStatus.SKIPPED
        )
    
    if not config.birdweather.api_token:
        logger.warning("No BirdWeather API token configured")
    
    logger.info("Starting BirdWeather ingestion")
    
    module = create_birdweather_module(
        api_token=config.birdweather.api_token,
        output_path=str(config.storage.base_path),
        target_species=config.birdweather.target_species
    )
    
    return module.run(
        start_date=start_date,
        end_date=end_date,
        incremental=config.incremental
    )


def run_xenocanto_ingestion(
    config: PipelineConfig,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None
) -> IngestionMetadata:
    """Run Xeno-Canto data ingestion."""
    logger = logging.getLogger("pipeline.xenocanto")
    
    if not config.xeno_canto.enabled:
        logger.info("Xeno-Canto ingestion is disabled")
        return IngestionMetadata(
            source_id="xeno_canto",
            batch_id="disabled",
            started_at=datetime.utcnow(),
            status=IngestionStatus.SKIPPED
        )
    
    logger.info("Starting Xeno-Canto ingestion")
    
    module = create_xenocanto_module(
        output_path=str(config.storage.base_path),
        country=config.xeno_canto.country_filter,
        quality=config.xeno_canto.quality_filter,
        download_audio=config.xeno_canto.download_audio
    )
    
    return module.run(
        start_date=start_date,
        end_date=end_date,
        incremental=config.incremental
    )


def run_knmi_ingestion(
    config: PipelineConfig,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None
) -> IngestionMetadata:
    """Run KNMI weather data ingestion."""
    logger = logging.getLogger("pipeline.knmi")
    
    if not config.knmi.enabled:
        logger.info("KNMI ingestion is disabled")
        return IngestionMetadata(
            source_id="knmi_weather",
            batch_id="disabled",
            started_at=datetime.utcnow(),
            status=IngestionStatus.SKIPPED
        )
    
    logger.info("Starting KNMI weather ingestion")
    
    module = create_knmi_module(
        api_key=config.knmi.api_key,
        output_path=str(config.storage.base_path),
        stations=config.knmi.stations if config.knmi.stations else None
    )
    
    return module.run(
        start_date=start_date,
        end_date=end_date,
        incremental=config.incremental
    )


def run_mortality_ingestion(
    config: PipelineConfig,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None
) -> IngestionMetadata:
    """Run mortality surveillance data ingestion."""
    logger = logging.getLogger("pipeline.mortality")
    
    if not config.mortality.enabled:
        logger.info("Mortality ingestion is disabled")
        return IngestionMetadata(
            source_id="mortality_surveillance",
            batch_id="disabled",
            started_at=datetime.utcnow(),
            status=IngestionStatus.SKIPPED
        )
    
    logger.info("Starting mortality surveillance ingestion")
    
    module = create_mortality_module(
        input_path=str(config.mortality.input_path),
        output_path=str(config.storage.base_path),
        data_format=config.mortality.data_format
    )
    
    return module.run(
        start_date=start_date,
        end_date=end_date,
        incremental=config.incremental
    )


def run_all_ingestion(
    config: PipelineConfig,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None
) -> Dict[str, IngestionMetadata]:
    """Run all enabled ingestion pipelines."""
    logger = logging.getLogger("pipeline.orchestrator")
    
    results = {}
    
    # BirdWeather (real-time acoustic data)
    logger.info("=" * 60)
    logger.info("BIRDWEATHER INGESTION")
    logger.info("=" * 60)
    results["birdweather"] = run_birdweather_ingestion(config, start_date, end_date)
    
    # KNMI Weather (environmental covariates)
    logger.info("=" * 60)
    logger.info("KNMI WEATHER INGESTION")
    logger.info("=" * 60)
    results["knmi"] = run_knmi_ingestion(config, start_date, end_date)
    
    # Mortality Surveillance (ground truth)
    logger.info("=" * 60)
    logger.info("MORTALITY SURVEILLANCE INGESTION")
    logger.info("=" * 60)
    results["mortality"] = run_mortality_ingestion(config, start_date, end_date)
    
    # Xeno-Canto (training data - typically run less frequently)
    if config.xeno_canto.enabled:
        logger.info("=" * 60)
        logger.info("XENO-CANTO INGESTION")
        logger.info("=" * 60)
        results["xeno_canto"] = run_xenocanto_ingestion(config, start_date, end_date)
    
    return results


def print_summary(results: Dict[str, IngestionMetadata]):
    """Print summary of ingestion results."""
    print("\n" + "=" * 70)
    print("INGESTION SUMMARY")
    print("=" * 70)
    
    total_fetched = 0
    total_validated = 0
    total_failed = 0
    
    for source, metadata in results.items():
        status_icon = {
            IngestionStatus.SUCCESS: "✓",
            IngestionStatus.PARTIAL: "⚠",
            IngestionStatus.FAILED: "✗",
            IngestionStatus.SKIPPED: "○"
        }.get(metadata.status, "?")
        
        duration = ""
        if metadata.completed_at and metadata.started_at:
            delta = metadata.completed_at - metadata.started_at
            duration = f" ({delta.total_seconds():.1f}s)"
        
        print(f"\n{status_icon} {source.upper()}{duration}")
        print(f"  Status: {metadata.status.value}")
        print(f"  Records fetched: {metadata.records_fetched}")
        print(f"  Records validated: {metadata.records_validated}")
        print(f"  Records failed: {metadata.records_failed}")
        
        if metadata.error_messages:
            print(f"  Errors: {len(metadata.error_messages)}")
            for err in metadata.error_messages[:3]:
                print(f"    - {err}")
        
        total_fetched += metadata.records_fetched
        total_validated += metadata.records_validated
        total_failed += metadata.records_failed
    
    print("\n" + "-" * 70)
    print(f"TOTAL: {total_fetched} fetched, {total_validated} validated, {total_failed} failed")
    print("=" * 70)


def show_status(config: PipelineConfig):
    """Show current pipeline status."""
    print("\n" + "=" * 70)
    print("AVIAN BIOSURVEILLANCE PIPELINE STATUS")
    print("=" * 70)
    
    print(f"\nStorage Path: {config.storage.base_path}")
    print(f"Incremental Mode: {config.incremental}")
    print(f"Log Level: {config.log_level}")
    
    print("\n--- Data Sources ---")
    
    sources = [
        ("BirdWeather", config.birdweather.enabled, "Real-time acoustic detections"),
        ("Xeno-Canto", config.xeno_canto.enabled, "Training audio corpus"),
        ("KNMI Weather", config.knmi.enabled, "Environmental covariates"),
        ("Mortality", config.mortality.enabled, "Surveillance ground truth"),
    ]
    
    for name, enabled, desc in sources:
        status = "✓ Enabled" if enabled else "○ Disabled"
        print(f"  {status} | {name}: {desc}")
    
    # Check for existing data
    print("\n--- Data Inventory ---")
    
    bronze_path = config.storage.bronze_path
    if bronze_path.exists():
        for source_dir in bronze_path.iterdir():
            if source_dir.is_dir():
                files = list(source_dir.rglob("*.json"))
                print(f"  {source_dir.name}: {len(files)} batch files")
    else:
        print("  No data ingested yet")
    
    # Check checkpoints
    print("\n--- Checkpoints ---")
    checkpoint_path = config.storage.checkpoint_path
    if checkpoint_path.exists():
        for cp_file in checkpoint_path.glob("*_checkpoint.json"):
            with open(cp_file) as f:
                cp_data = json.load(f)
            last_ts = cp_data.get("last_timestamp", "unknown")
            print(f"  {cp_file.stem}: {last_ts}")
    else:
        print("  No checkpoints found")
    
    print("=" * 70)


def main():
    """Main entry point for the pipeline."""
    parser = argparse.ArgumentParser(
        description="Avian Biosurveillance Data Ingestion Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_pipeline.py ingest --source birdweather
  python run_pipeline.py ingest --all --days 7
  python run_pipeline.py status
  python run_pipeline.py init-config
        """
    )
    
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config.yaml"),
        help="Path to configuration file"
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Command to run")
    
    # Ingest command
    ingest_parser = subparsers.add_parser("ingest", help="Run data ingestion")
    ingest_parser.add_argument(
        "--source",
        choices=["birdweather", "xenocanto", "knmi", "mortality"],
        help="Specific source to ingest"
    )
    ingest_parser.add_argument(
        "--all",
        action="store_true",
        help="Ingest from all enabled sources"
    )
    ingest_parser.add_argument(
        "--days",
        type=int,
        default=1,
        help="Number of days to fetch (default: 1)"
    )
    ingest_parser.add_argument(
        "--full",
        action="store_true",
        help="Run full (non-incremental) ingestion"
    )
    
    # Status command
    subparsers.add_parser("status", help="Show pipeline status")
    
    # Init config command
    subparsers.add_parser("init-config", help="Create default configuration file")
    
    args = parser.parse_args()
    
    if args.command == "init-config":
        create_default_config(args.config)
        return
    
    # Load configuration
    if args.config.exists():
        config = PipelineConfig.from_file(args.config)
    else:
        print(f"Config file not found: {args.config}")
        print("Loading configuration from environment variables...")
        config = PipelineConfig.from_env()
    
    # Setup logging
    setup_logging(config.log_level)
    logger = logging.getLogger("pipeline")
    
    # Ensure storage directories exist
    config.storage.ensure_directories()
    
    if args.command == "status":
        show_status(config)
        return
    
    if args.command == "ingest":
        # Override incremental if --full specified
        if hasattr(args, 'full') and args.full:
            config.incremental = False
        
        # Calculate date range
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=args.days)
        
        logger.info(f"Starting ingestion: {start_date.date()} to {end_date.date()}")
        
        if args.all:
            results = run_all_ingestion(config, start_date, end_date)
            print_summary(results)
        elif args.source:
            source_runners = {
                "birdweather": run_birdweather_ingestion,
                "xenocanto": run_xenocanto_ingestion,
                "knmi": run_knmi_ingestion,
                "mortality": run_mortality_ingestion
            }
            
            runner = source_runners.get(args.source)
            if runner:
                result = runner(config, start_date, end_date)
                print_summary({args.source: result})
            else:
                print(f"Unknown source: {args.source}")
                sys.exit(1)
        else:
            print("Specify --source or --all")
            ingest_parser.print_help()
            sys.exit(1)
    
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
