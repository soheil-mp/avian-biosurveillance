"""
Mortality Surveillance Ingestion Module
=======================================
Handles dead bird report data from Dutch Wildlife Health Centre (DWHC)
and Sovon Vogelonderzoek Nederland.
"""

import csv
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional
import hashlib

from .base import BaseIngestionModule, DataRecord

logger = logging.getLogger(__name__)


# Dutch provinces mapping
DUTCH_PROVINCES = {
    "DR": "Drenthe",
    "FL": "Flevoland",
    "FR": "Friesland",
    "GE": "Gelderland",
    "GR": "Groningen",
    "LI": "Limburg",
    "NB": "Noord-Brabant",
    "NH": "Noord-Holland",
    "OV": "Overijssel",
    "UT": "Utrecht",
    "ZE": "Zeeland",
    "ZH": "Zuid-Holland"
}

# USUV test result codes
USUV_RESULT_CODES = {
    "POS": True,
    "POSITIVE": True,
    "NEG": False,
    "NEGATIVE": False,
    "NT": None,  # Not tested
    "INC": None,  # Inconclusive
}

# Target species eBird codes
SPECIES_MAPPING = {
    "Turdus merula": "eurbla",
    "Merel": "eurbla",
    "Eurasian Blackbird": "eurbla",
    "Turdus philomelos": "sonthr1",
    "Zanglijster": "sonthr1",
    "Song Thrush": "sonthr1",
    "Pica pica": "eurmag1",
    "Ekster": "eurmag1",
    "Eurasian Magpie": "eurmag1",
    "Garrulus glandarius": "eurjay1",
    "Vlaamse gaai": "eurjay1",
    "Eurasian Jay": "eurjay1",
}


class MortalitySurveillanceIngestion(BaseIngestionModule):
    """
    Ingestion module for bird mortality surveillance data.
    
    Supports two data formats:
    1. DWHC pathology reports (structured lab results)
    2. Sovon citizen reports (field observations)
    
    Data access requires formal data sharing agreements with these organizations.
    """
    
    SUPPORTED_FORMATS = ["dwhc_csv", "sovon_csv", "json"]
    
    def __init__(
        self,
        config: Dict[str, Any],
        output_path: Path,
        checkpoint_path: Optional[Path] = None
    ):
        super().__init__(
            source_id="mortality_surveillance",
            config=config,
            output_path=output_path,
            checkpoint_path=checkpoint_path
        )
        
        self.data_format = config.get("data_format", "dwhc_csv")
        self.input_path = Path(config.get("input_path", "./input"))
        self.target_species = config.get("target_species", list(SPECIES_MAPPING.keys()))
        
        # Validate format
        if self.data_format not in self.SUPPORTED_FORMATS:
            raise ValueError(f"Unsupported format: {self.data_format}")
    
    def connect(self) -> bool:
        """Verify input directory exists and contains data files."""
        if not self.input_path.exists():
            logger.warning(f"Input path does not exist: {self.input_path}")
            # Create directory for future use
            self.input_path.mkdir(parents=True, exist_ok=True)
            return True
        
        # Check for data files
        data_files = list(self.input_path.glob("*.csv")) + list(self.input_path.glob("*.json"))
        logger.info(f"Found {len(data_files)} data files in {self.input_path}")
        
        return True
    
    def _parse_dwhc_csv(self, file_path: Path) -> Generator[Dict[str, Any], None, None]:
        """Parse DWHC pathology report CSV format."""
        try:
            with open(file_path, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f, delimiter=';')
                
                for row in reader:
                    yield {
                        "case_id": row.get("case_id", row.get("CaseID", "")),
                        "species_reported": row.get("species", row.get("Species", "")),
                        "report_date": row.get("date_found", row.get("DateFound", "")),
                        "submission_date": row.get("date_submitted", row.get("DateSubmitted", "")),
                        "latitude": row.get("latitude", row.get("Lat", "")),
                        "longitude": row.get("longitude", row.get("Lon", "")),
                        "province": row.get("province", row.get("Province", "")),
                        "municipality": row.get("municipality", row.get("Municipality", "")),
                        "usuv_tested": row.get("usuv_tested", row.get("USUV_Tested", "")),
                        "usuv_result": row.get("usuv_result", row.get("USUV_Result", "")),
                        "pathology_findings": row.get("pathology", row.get("Pathology", "")),
                        "cause_of_death": row.get("cause_death", row.get("CauseOfDeath", "")),
                        "co_infections": row.get("co_infections", row.get("CoInfections", "")),
                        "reporter_type": "dwhc",
                        "_source_file": file_path.name
                    }
                    
        except Exception as e:
            logger.error(f"Error parsing DWHC CSV {file_path}: {e}")
    
    def _parse_sovon_csv(self, file_path: Path) -> Generator[Dict[str, Any], None, None]:
        """Parse Sovon citizen science report CSV format."""
        try:
            with open(file_path, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f, delimiter=';')
                
                for row in reader:
                    yield {
                        "report_id": row.get("report_id", row.get("ReportID", "")),
                        "species_reported": row.get("species", row.get("Soort", "")),
                        "report_date": row.get("date", row.get("Datum", "")),
                        "latitude": row.get("lat", row.get("Latitude", "")),
                        "longitude": row.get("lon", row.get("Longitude", "")),
                        "province": row.get("province", row.get("Provincie", "")),
                        "count": row.get("count", row.get("Aantal", "1")),
                        "description": row.get("description", row.get("Omschrijving", "")),
                        "observer_notes": row.get("notes", row.get("Opmerkingen", "")),
                        "usuv_tested": "NT",  # Citizen reports typically not tested
                        "usuv_result": None,
                        "reporter_type": "sovon",
                        "_source_file": file_path.name
                    }
                    
        except Exception as e:
            logger.error(f"Error parsing Sovon CSV {file_path}: {e}")
    
    def _parse_json_file(self, file_path: Path) -> Generator[Dict[str, Any], None, None]:
        """Parse JSON format mortality data."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            records = data if isinstance(data, list) else data.get("records", [data])
            
            for record in records:
                record["_source_file"] = file_path.name
                yield record
                
        except Exception as e:
            logger.error(f"Error parsing JSON {file_path}: {e}")
    
    def fetch(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        input_files: Optional[List[Path]] = None,
        **kwargs
    ) -> Generator[DataRecord, None, None]:
        """
        Fetch mortality records from input files.
        
        Args:
            start_date: Filter records after this date.
            end_date: Filter records before this date.
            input_files: Specific files to process (None for all in input_path).
            
        Yields:
            DataRecord for each mortality report.
        """
        # Determine files to process
        if input_files:
            files = [Path(f) for f in input_files]
        else:
            files = list(self.input_path.glob("*.csv")) + list(self.input_path.glob("*.json"))
        
        logger.info(f"Processing {len(files)} input files")
        
        for file_path in files:
            # Determine parser based on format config or file extension
            if file_path.suffix == ".json":
                parser = self._parse_json_file
            elif "dwhc" in file_path.name.lower() or self.data_format == "dwhc_csv":
                parser = self._parse_dwhc_csv
            else:
                parser = self._parse_sovon_csv
            
            for raw_record in parser(file_path):
                # Date filtering
                date_str = raw_record.get("report_date", "")
                if date_str:
                    try:
                        # Try multiple date formats
                        for fmt in ["%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%Y%m%d"]:
                            try:
                                record_date = datetime.strptime(date_str, fmt)
                                break
                            except ValueError:
                                continue
                        else:
                            record_date = None
                        
                        if record_date:
                            if start_date and record_date < start_date:
                                continue
                            if end_date and record_date > end_date:
                                continue
                    except Exception:
                        pass
                
                # Generate record ID
                record_id = raw_record.get("case_id") or raw_record.get("report_id")
                if not record_id:
                    # Create hash-based ID
                    content = json.dumps(raw_record, sort_keys=True)
                    record_id = hashlib.md5(content.encode()).hexdigest()[:12]
                
                yield DataRecord(
                    record_id=record_id,
                    source_system="mortality_surveillance",
                    ingestion_timestamp=datetime.utcnow(),
                    raw_data=raw_record
                )
    
    def validate(self, record: DataRecord) -> DataRecord:
        """Validate mortality surveillance record."""
        errors = []
        data = record.raw_data
        
        # Required fields
        if not data.get("species_reported"):
            errors.append("Missing species identification")
        
        if not data.get("report_date"):
            errors.append("Missing report date")
        
        # Location validation
        lat = data.get("latitude")
        lon = data.get("longitude")
        
        if lat and lon:
            try:
                lat_f = float(lat)
                lon_f = float(lon)
                
                # Netherlands bounds
                if not (50.75 <= lat_f <= 53.47):
                    errors.append(f"Latitude {lat_f} outside Netherlands")
                if not (3.37 <= lon_f <= 7.21):
                    errors.append(f"Longitude {lon_f} outside Netherlands")
            except (ValueError, TypeError):
                errors.append("Invalid coordinate format")
        
        # Date validation
        date_str = data.get("report_date", "")
        if date_str:
            valid_date = False
            for fmt in ["%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%Y%m%d"]:
                try:
                    parsed = datetime.strptime(date_str, fmt)
                    if parsed > datetime.utcnow():
                        errors.append("Report date is in the future")
                    if parsed.year < 2015:
                        errors.append("Report date before USUV surveillance period")
                    valid_date = True
                    break
                except ValueError:
                    continue
            
            if not valid_date:
                errors.append(f"Could not parse date: {date_str}")
        
        record.validated = len(errors) == 0
        record.validation_errors = errors
        
        return record
    
    def transform(self, record: DataRecord) -> Dict[str, Any]:
        """Transform mortality record to standardized schema."""
        data = record.raw_data
        
        # Parse coordinates
        lat = None
        lon = None
        try:
            if data.get("latitude"):
                lat = float(data["latitude"])
            if data.get("longitude"):
                lon = float(data["longitude"])
        except (ValueError, TypeError):
            pass
        
        # Parse date
        report_date = None
        date_str = data.get("report_date", "")
        for fmt in ["%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%Y%m%d"]:
            try:
                report_date = datetime.strptime(date_str, fmt)
                break
            except ValueError:
                continue
        
        # Normalize species name
        species_reported = data.get("species_reported", "")
        species_code = SPECIES_MAPPING.get(species_reported, "")
        
        # Normalize USUV result
        usuv_tested_raw = str(data.get("usuv_tested", "")).upper()
        usuv_result_raw = str(data.get("usuv_result", "")).upper()
        
        usuv_tested = usuv_tested_raw in ["YES", "TRUE", "1", "POS", "NEG", "POSITIVE", "NEGATIVE"]
        usuv_positive = USUV_RESULT_CODES.get(usuv_result_raw)
        
        # Normalize province
        province_raw = data.get("province", "")
        province = DUTCH_PROVINCES.get(province_raw.upper(), province_raw)
        
        return {
            "report_id": record.record_id,
            "report_date": report_date.strftime("%Y-%m-%d") if report_date else None,
            "species_reported": species_reported,
            "species_code": species_code,
            "latitude": lat,
            "longitude": lon,
            "province": province,
            "municipality": data.get("municipality", ""),
            "count": int(data.get("count", 1)) if data.get("count") else 1,
            "usuv_tested": usuv_tested,
            "usuv_positive": usuv_positive,
            "pathology_notes": data.get("pathology_findings", ""),
            "cause_of_death": data.get("cause_of_death", ""),
            "co_infections": data.get("co_infections", ""),
            "reporter_type": data.get("reporter_type", "unknown"),
            "source_network": "DWHC" if data.get("reporter_type") == "dwhc" else "Sovon",
            "source_system": "mortality_surveillance",
            "_source_file": data.get("_source_file", ""),
            "_raw_hash": record.compute_hash(),
            "_ingestion_timestamp": record.ingestion_timestamp.isoformat(),
            "_batch_id": self._batch_id
        }


def create_mortality_module(
    input_path: str,
    output_path: str = "./data",
    data_format: str = "dwhc_csv"
) -> MortalitySurveillanceIngestion:
    """
    Factory function to create mortality surveillance ingestion module.
    
    Args:
        input_path: Path to directory containing input data files.
        output_path: Base path for output data.
        data_format: Expected data format (dwhc_csv, sovon_csv, json).
        
    Returns:
        Configured MortalitySurveillanceIngestion instance.
    """
    config = {
        "input_path": input_path,
        "data_format": data_format,
        "target_species": list(SPECIES_MAPPING.keys())
    }
    
    return MortalitySurveillanceIngestion(
        config=config,
        output_path=Path(output_path)
    )


if __name__ == "__main__":
    # Example usage
    module = create_mortality_module(
        input_path="./data/mortality_reports",
        output_path="./data/biosurveillance",
        data_format="dwhc_csv"
    )
    
    result = module.run(incremental=False)
    print(f"Ingestion completed: {result.to_dict()}")
