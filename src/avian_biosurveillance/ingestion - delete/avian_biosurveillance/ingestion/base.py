"""
Base Ingestion Module for Avian Biosurveillance System
======================================================
Abstract base class defining the interface for all data source ingestion modules.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional
import hashlib
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class IngestionStatus(Enum):
    """Status codes for ingestion operations."""
    SUCCESS = "success"
    PARTIAL = "partial"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class IngestionMetadata:
    """Metadata for tracking ingestion runs."""
    source_id: str
    batch_id: str
    started_at: datetime
    completed_at: Optional[datetime] = None
    status: IngestionStatus = IngestionStatus.SUCCESS
    records_fetched: int = 0
    records_validated: int = 0
    records_failed: int = 0
    error_messages: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "source_id": self.source_id,
            "batch_id": self.batch_id,
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "status": self.status.value,
            "records_fetched": self.records_fetched,
            "records_validated": self.records_validated,
            "records_failed": self.records_failed,
            "error_messages": self.error_messages
        }


@dataclass
class DataRecord:
    """Generic container for ingested data records."""
    record_id: str
    source_system: str
    ingestion_timestamp: datetime
    raw_data: Dict[str, Any]
    validated: bool = False
    validation_errors: List[str] = field(default_factory=list)
    
    def compute_hash(self) -> str:
        """Generate content hash for deduplication."""
        content = json.dumps(self.raw_data, sort_keys=True)
        return hashlib.sha256(content.encode()).hexdigest()


class BaseIngestionModule(ABC):
    """
    Abstract base class for data source ingestion modules.
    
    All source-specific ingestion modules must inherit from this class
    and implement the abstract methods.
    """
    
    def __init__(
        self,
        source_id: str,
        config: Dict[str, Any],
        output_path: Path,
        checkpoint_path: Optional[Path] = None
    ):
        self.source_id = source_id
        self.config = config
        self.output_path = Path(output_path)
        self.checkpoint_path = checkpoint_path or self.output_path / "checkpoints"
        self.output_path.mkdir(parents=True, exist_ok=True)
        self.checkpoint_path.mkdir(parents=True, exist_ok=True)
        self._batch_id = self._generate_batch_id()
        
    def _generate_batch_id(self) -> str:
        """Generate unique batch identifier."""
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        return f"{self.source_id}_{timestamp}"
    
    @abstractmethod
    def connect(self) -> bool:
        """
        Establish connection to data source.
        
        Returns:
            bool: True if connection successful, False otherwise.
        """
        pass
    
    @abstractmethod
    def fetch(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        **kwargs
    ) -> Generator[DataRecord, None, None]:
        """
        Fetch data records from source.
        
        Args:
            start_date: Start of time range to fetch.
            end_date: End of time range to fetch.
            **kwargs: Source-specific parameters.
            
        Yields:
            DataRecord: Individual data records.
        """
        pass
    
    @abstractmethod
    def validate(self, record: DataRecord) -> DataRecord:
        """
        Validate a single data record.
        
        Args:
            record: The record to validate.
            
        Returns:
            DataRecord: Record with validation status updated.
        """
        pass
    
    @abstractmethod
    def transform(self, record: DataRecord) -> Dict[str, Any]:
        """
        Transform raw record to standardized schema.
        
        Args:
            record: Validated data record.
            
        Returns:
            Dict containing transformed data in target schema.
        """
        pass
    
    def load(self, records: List[Dict[str, Any]], partition_key: str) -> Path:
        """
        Write transformed records to bronze layer.
        
        Args:
            records: List of transformed records.
            partition_key: Date-based partition key (YYYY-MM-DD).
            
        Returns:
            Path to written file.
        """
        output_dir = self.output_path / "bronze" / self.source_id / partition_key
        output_dir.mkdir(parents=True, exist_ok=True)
        
        output_file = output_dir / f"{self._batch_id}.json"
        
        with open(output_file, 'w') as f:
            json.dump({
                "batch_id": self._batch_id,
                "source_id": self.source_id,
                "ingestion_timestamp": datetime.utcnow().isoformat(),
                "record_count": len(records),
                "records": records
            }, f, indent=2, default=str)
        
        logger.info(f"Wrote {len(records)} records to {output_file}")
        return output_file
    
    def save_checkpoint(self, checkpoint_data: Dict[str, Any]) -> None:
        """Save checkpoint for incremental processing."""
        checkpoint_file = self.checkpoint_path / f"{self.source_id}_checkpoint.json"
        checkpoint_data["updated_at"] = datetime.utcnow().isoformat()
        
        with open(checkpoint_file, 'w') as f:
            json.dump(checkpoint_data, f, indent=2)
        
        logger.info(f"Saved checkpoint to {checkpoint_file}")
    
    def load_checkpoint(self) -> Optional[Dict[str, Any]]:
        """Load last checkpoint for incremental processing."""
        checkpoint_file = self.checkpoint_path / f"{self.source_id}_checkpoint.json"
        
        if checkpoint_file.exists():
            with open(checkpoint_file, 'r') as f:
                return json.load(f)
        return None
    
    def run(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        incremental: bool = True,
        **kwargs
    ) -> IngestionMetadata:
        """
        Execute full ingestion pipeline.
        
        Args:
            start_date: Start of time range (None for checkpoint-based).
            end_date: End of time range (None for now).
            incremental: Whether to use checkpointing.
            **kwargs: Source-specific parameters.
            
        Returns:
            IngestionMetadata with run statistics.
        """
        metadata = IngestionMetadata(
            source_id=self.source_id,
            batch_id=self._batch_id,
            started_at=datetime.utcnow()
        )
        
        try:
            # Connect to source
            if not self.connect():
                metadata.status = IngestionStatus.FAILED
                metadata.error_messages.append("Failed to connect to data source")
                return metadata
            
            # Determine time range
            if incremental and start_date is None:
                checkpoint = self.load_checkpoint()
                if checkpoint and "last_timestamp" in checkpoint:
                    start_date = datetime.fromisoformat(checkpoint["last_timestamp"])
            
            end_date = end_date or datetime.utcnow()
            
            # Fetch and process records
            validated_records = []
            latest_timestamp = None
            
            for record in self.fetch(start_date, end_date, **kwargs):
                metadata.records_fetched += 1
                
                # Validate
                record = self.validate(record)
                if record.validated:
                    metadata.records_validated += 1
                    
                    # Transform
                    transformed = self.transform(record)
                    validated_records.append(transformed)
                    
                    # Track latest timestamp for checkpoint
                    if "timestamp" in transformed:
                        ts = transformed["timestamp"]
                        if isinstance(ts, str):
                            ts = datetime.fromisoformat(ts.replace('Z', '+00:00'))
                        if latest_timestamp is None or ts > latest_timestamp:
                            latest_timestamp = ts
                else:
                    metadata.records_failed += 1
                    metadata.error_messages.extend(record.validation_errors[:5])  # Limit errors
            
            # Load to bronze layer
            if validated_records:
                partition_key = end_date.strftime("%Y-%m-%d")
                self.load(validated_records, partition_key)
            
            # Save checkpoint
            if incremental and latest_timestamp:
                self.save_checkpoint({"last_timestamp": latest_timestamp.isoformat()})
            
            # Set final status
            if metadata.records_failed == 0:
                metadata.status = IngestionStatus.SUCCESS
            elif metadata.records_validated > 0:
                metadata.status = IngestionStatus.PARTIAL
            else:
                metadata.status = IngestionStatus.FAILED
                
        except Exception as e:
            logger.exception(f"Ingestion failed: {e}")
            metadata.status = IngestionStatus.FAILED
            metadata.error_messages.append(str(e))
        
        metadata.completed_at = datetime.utcnow()
        return metadata
