"""
Avian Biosurveillance System - Data Ingestion Package
=====================================================
Modules for downloading and processing data from various sources
for disease outbreak detection in wild bird populations.
"""

from .base import (
    BaseIngestionModule,
    DataRecord,
    IngestionMetadata,
    IngestionStatus
)

from .birdweather import (
    BirdWeatherIngestion,
    create_birdweather_module
)

from .xeno_canto import (
    XenoCantoIngestion,
    create_xenocanto_module
)

from .knmi_weather import (
    KNMIWeatherIngestion,
    create_knmi_module
)

from .mortality_surveillance import (
    MortalitySurveillanceIngestion,
    create_mortality_module
)

__all__ = [
    # Base classes
    "BaseIngestionModule",
    "DataRecord",
    "IngestionMetadata",
    "IngestionStatus",
    
    # BirdWeather
    "BirdWeatherIngestion",
    "create_birdweather_module",
    
    # Xeno-Canto
    "XenoCantoIngestion",
    "create_xenocanto_module",
    
    # KNMI Weather
    "KNMIWeatherIngestion",
    "create_knmi_module",
    
    # Mortality Surveillance
    "MortalitySurveillanceIngestion",
    "create_mortality_module",
]

__version__ = "1.0.0"
