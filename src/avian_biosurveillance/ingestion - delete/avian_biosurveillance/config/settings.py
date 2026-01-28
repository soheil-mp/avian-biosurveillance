"""
Configuration Management for Avian Biosurveillance System
=========================================================
Centralized configuration with environment variable support.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional
import json
import yaml


@dataclass
class StorageConfig:
    """Storage layer configuration."""
    base_path: Path = Path("./data/biosurveillance")
    bronze_path: Path = field(init=False)
    silver_path: Path = field(init=False)
    gold_path: Path = field(init=False)
    raw_audio_path: Path = field(init=False)
    checkpoint_path: Path = field(init=False)
    
    def __post_init__(self):
        self.bronze_path = self.base_path / "bronze"
        self.silver_path = self.base_path / "silver"
        self.gold_path = self.base_path / "gold"
        self.raw_audio_path = self.base_path / "raw_audio"
        self.checkpoint_path = self.base_path / "checkpoints"
    
    def ensure_directories(self):
        """Create all storage directories."""
        for path in [self.bronze_path, self.silver_path, self.gold_path,
                     self.raw_audio_path, self.checkpoint_path]:
            path.mkdir(parents=True, exist_ok=True)


@dataclass
class BirdWeatherConfig:
    """BirdWeather API configuration."""
    api_token: str = ""
    rate_limit_delay: float = 0.5
    page_size: int = 100
    enabled: bool = True
    
    # Netherlands geographic filter
    min_lat: float = 50.75
    max_lat: float = 53.47
    min_lon: float = 3.37
    max_lon: float = 7.21
    
    # Target species (eBird codes)
    target_species: List[str] = field(default_factory=lambda: [
        "eurbla", "sonthr1", "eurgrn1", "eurmag1", "eurjay1"
    ])
    
    @classmethod
    def from_env(cls) -> "BirdWeatherConfig":
        """Load configuration from environment variables."""
        return cls(
            api_token=os.environ.get("BIRDWEATHER_API_TOKEN", ""),
            rate_limit_delay=float(os.environ.get("BIRDWEATHER_RATE_LIMIT", "0.5")),
            page_size=int(os.environ.get("BIRDWEATHER_PAGE_SIZE", "100")),
            enabled=os.environ.get("BIRDWEATHER_ENABLED", "true").lower() == "true"
        )


@dataclass
class XenoCantoConfig:
    """Xeno-Canto repository configuration."""
    country_filter: str = "Netherlands"
    quality_filter: str = "B"  # A, B, C, D, E (A is best)
    download_audio: bool = False
    rate_limit_delay: float = 1.0
    enabled: bool = True
    
    @classmethod
    def from_env(cls) -> "XenoCantoConfig":
        return cls(
            country_filter=os.environ.get("XENOCANTO_COUNTRY", "Netherlands"),
            quality_filter=os.environ.get("XENOCANTO_QUALITY", "B"),
            download_audio=os.environ.get("XENOCANTO_DOWNLOAD_AUDIO", "false").lower() == "true",
            enabled=os.environ.get("XENOCANTO_ENABLED", "true").lower() == "true"
        )


@dataclass
class KNMIConfig:
    """KNMI weather data configuration."""
    api_key: str = ""
    rate_limit_delay: float = 0.5
    enabled: bool = True
    
    # Default stations (major Netherlands weather stations)
    stations: List[str] = field(default_factory=lambda: [
        "260", "240", "344", "370", "380", "270", "280", "290", "375", "350"
    ])
    
    @classmethod
    def from_env(cls) -> "KNMIConfig":
        return cls(
            api_key=os.environ.get("KNMI_API_KEY", ""),
            enabled=os.environ.get("KNMI_ENABLED", "true").lower() == "true"
        )


@dataclass
class MortalityConfig:
    """Mortality surveillance configuration."""
    input_path: Path = Path("./data/mortality_reports")
    data_format: str = "dwhc_csv"
    enabled: bool = True
    
    @classmethod
    def from_env(cls) -> "MortalityConfig":
        return cls(
            input_path=Path(os.environ.get("MORTALITY_INPUT_PATH", "./data/mortality_reports")),
            data_format=os.environ.get("MORTALITY_FORMAT", "dwhc_csv"),
            enabled=os.environ.get("MORTALITY_ENABLED", "true").lower() == "true"
        )


@dataclass
class PipelineConfig:
    """Overall pipeline configuration."""
    storage: StorageConfig = field(default_factory=StorageConfig)
    birdweather: BirdWeatherConfig = field(default_factory=BirdWeatherConfig)
    xeno_canto: XenoCantoConfig = field(default_factory=XenoCantoConfig)
    knmi: KNMIConfig = field(default_factory=KNMIConfig)
    mortality: MortalityConfig = field(default_factory=MortalityConfig)
    
    # Pipeline settings
    incremental: bool = True
    parallel_ingestion: bool = False
    log_level: str = "INFO"
    
    @classmethod
    def from_env(cls) -> "PipelineConfig":
        """Load full configuration from environment variables."""
        storage_path = os.environ.get("BIOSURVEILLANCE_DATA_PATH", "./data/biosurveillance")
        
        return cls(
            storage=StorageConfig(base_path=Path(storage_path)),
            birdweather=BirdWeatherConfig.from_env(),
            xeno_canto=XenoCantoConfig.from_env(),
            knmi=KNMIConfig.from_env(),
            mortality=MortalityConfig.from_env(),
            incremental=os.environ.get("PIPELINE_INCREMENTAL", "true").lower() == "true",
            parallel_ingestion=os.environ.get("PIPELINE_PARALLEL", "false").lower() == "true",
            log_level=os.environ.get("LOG_LEVEL", "INFO")
        )
    
    @classmethod
    def from_file(cls, config_path: Path) -> "PipelineConfig":
        """Load configuration from YAML or JSON file."""
        with open(config_path, 'r') as f:
            if config_path.suffix in ['.yaml', '.yml']:
                data = yaml.safe_load(f)
            else:
                data = json.load(f)
        
        return cls._from_dict(data)
    
    @classmethod
    def _from_dict(cls, data: Dict[str, Any]) -> "PipelineConfig":
        """Build configuration from dictionary."""
        storage_data = data.get("storage", {})
        storage = StorageConfig(
            base_path=Path(storage_data.get("base_path", "./data/biosurveillance"))
        )
        
        bw_data = data.get("birdweather", {})
        birdweather = BirdWeatherConfig(
            api_token=bw_data.get("api_token", ""),
            rate_limit_delay=bw_data.get("rate_limit_delay", 0.5),
            page_size=bw_data.get("page_size", 100),
            enabled=bw_data.get("enabled", True),
            target_species=bw_data.get("target_species", ["eurbla", "sonthr1"])
        )
        
        xc_data = data.get("xeno_canto", {})
        xeno_canto = XenoCantoConfig(
            country_filter=xc_data.get("country_filter", "Netherlands"),
            quality_filter=xc_data.get("quality_filter", "B"),
            download_audio=xc_data.get("download_audio", False),
            enabled=xc_data.get("enabled", True)
        )
        
        knmi_data = data.get("knmi", {})
        knmi = KNMIConfig(
            api_key=knmi_data.get("api_key", ""),
            stations=knmi_data.get("stations", []),
            enabled=knmi_data.get("enabled", True)
        )
        
        mort_data = data.get("mortality", {})
        mortality = MortalityConfig(
            input_path=Path(mort_data.get("input_path", "./data/mortality_reports")),
            data_format=mort_data.get("data_format", "dwhc_csv"),
            enabled=mort_data.get("enabled", True)
        )
        
        return cls(
            storage=storage,
            birdweather=birdweather,
            xeno_canto=xeno_canto,
            knmi=knmi,
            mortality=mortality,
            incremental=data.get("incremental", True),
            parallel_ingestion=data.get("parallel_ingestion", False),
            log_level=data.get("log_level", "INFO")
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Export configuration to dictionary."""
        return {
            "storage": {
                "base_path": str(self.storage.base_path)
            },
            "birdweather": {
                "api_token": "***" if self.birdweather.api_token else "",
                "rate_limit_delay": self.birdweather.rate_limit_delay,
                "page_size": self.birdweather.page_size,
                "enabled": self.birdweather.enabled,
                "target_species": self.birdweather.target_species
            },
            "xeno_canto": {
                "country_filter": self.xeno_canto.country_filter,
                "quality_filter": self.xeno_canto.quality_filter,
                "download_audio": self.xeno_canto.download_audio,
                "enabled": self.xeno_canto.enabled
            },
            "knmi": {
                "api_key": "***" if self.knmi.api_key else "",
                "stations": self.knmi.stations,
                "enabled": self.knmi.enabled
            },
            "mortality": {
                "input_path": str(self.mortality.input_path),
                "data_format": self.mortality.data_format,
                "enabled": self.mortality.enabled
            },
            "incremental": self.incremental,
            "parallel_ingestion": self.parallel_ingestion,
            "log_level": self.log_level
        }
    
    def save(self, config_path: Path):
        """Save configuration to file."""
        data = self.to_dict()
        
        with open(config_path, 'w') as f:
            if config_path.suffix in ['.yaml', '.yml']:
                yaml.dump(data, f, default_flow_style=False)
            else:
                json.dump(data, f, indent=2)


# Default configuration template
DEFAULT_CONFIG_YAML = """
# Avian Biosurveillance System Configuration
# ==========================================

storage:
  base_path: ./data/biosurveillance

birdweather:
  api_token: ""  # Set via BIRDWEATHER_API_TOKEN env var
  rate_limit_delay: 0.5
  page_size: 100
  enabled: true
  target_species:
    - eurbla      # Eurasian Blackbird
    - sonthr1     # Song Thrush
    - eurgrn1     # European Greenfinch
    - eurmag1     # Eurasian Magpie
    - eurjay1     # Eurasian Jay

xeno_canto:
  country_filter: Netherlands
  quality_filter: B
  download_audio: false
  enabled: true

knmi:
  api_key: ""  # Set via KNMI_API_KEY env var
  enabled: true
  stations:
    - "260"   # De Bilt
    - "240"   # Schiphol
    - "344"   # Rotterdam
    - "370"   # Eindhoven
    - "380"   # Maastricht

mortality:
  input_path: ./data/mortality_reports
  data_format: dwhc_csv
  enabled: true

incremental: true
parallel_ingestion: false
log_level: INFO
"""


def create_default_config(config_path: Path):
    """Create default configuration file."""
    config_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(config_path, 'w') as f:
        f.write(DEFAULT_CONFIG_YAML)
    
    print(f"Created default configuration at {config_path}")
