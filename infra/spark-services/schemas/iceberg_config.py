"""Iceberg configuration - per-layer settings (Bronze only for now).

Centralized & extensible for:
- Time travel (metadata retention)
- Tuning (file size, compression, etc.)
- Partitioning strategy
- Data format & compression

Future: Add SILVER1_CONFIG, SILVER2_CONFIG, GOLD_CONFIG when implementing those layers.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class IcebergConfig:
    """Iceberg table configuration - extensible per-layer."""
    
    # ============ TIME TRAVEL ============
    # Keep snapshots for N days (time-based retention)
    metadata_max_age_days: int = 30
    
    # Keep last N metadata file versions (count-based retention)
    metadata_previous_versions_max: int = 0
    
    # ============ TUNING ============
    target_file_size_bytes: int = 268435456  # 256 MB default
    
    # Compression codec: snappy, gzip, zstd, uncompressed
    # snappy = fast, zstd = best compression
    compression_codec: str = "snappy"
    
    # ============ PARTITIONING ============
    partition_spec: str = "days(event_time), event_source"
    
    # ============ FORMAT & SCHEMA ============
    # Data format: parquet (default), orc, etc.
    format_version: str = "2"  # v2 supports more features
    write_format_default: str = "parquet"
    
    # ============ STATISTICS ============
    # Enable column statistics for query optimization
    write_statistics_enabled: bool = True
    
    # ============ SORTING ============
    # Z-order clustering columns (for query performance)
    # Empty = no clustering
    z_order_columns: list[str] = field(default_factory=list)
    
    # ============ ICEBERG SPECIFIC ============
    # Allow partial deletes (needed for SCD2 in Silver)
    delete_mode: str = "position-deletes"  # vs copy-on-write
    
    # Distribution mode for small files handling
    # "range" (default) = single writer per partition
    # "hash" = multiple writers per partition, auto-coalesce
    distribution_mode: str = "hash"
    
    def schema_properties(self) -> dict[str, str]:
        """Generate TBLPROPERTIES dict for CREATE TABLE.
        
        Metadata retention is split into 2 concepts:
        - history.expire.max-snapshot-age-ms: Keep snapshots for N days (time-based)
        - write.metadata.previous-versions-max: Keep last N metadata files (count-based)
        """
        props = {
            "format-version": self.format_version,
            "write.format.default": self.write_format_default,
            "write.parquet.compression-codec": self.compression_codec,
            "write.target-file-size-bytes": str(self.target_file_size_bytes),
            "write.delete.mode": self.delete_mode,
            "write.distribution-mode": self.distribution_mode,
        }
        
        if self.metadata_max_age_days > 0:
            age_ms = self.metadata_max_age_days * 24 * 3600 * 1000
            props["history.expire.max-snapshot-age-ms"] = str(age_ms)
        
        if self.metadata_previous_versions_max > 0:
            props["write.metadata.previous-versions-max"] = str(
                self.metadata_previous_versions_max
            )
        
        # Statistics
        if self.write_statistics_enabled:
            props["write.stats.mode"] = "full"
        
        return props
    
    def partition_clause(self) -> str:
        """Generate PARTITIONED BY clause."""
        return f"PARTITIONED BY ({self.partition_spec})"
    
    def z_order_clause(self) -> str:
        """Generate Z-order clustering clause (if applicable)."""
        if not self.z_order_columns:
            return ""
        cols = ", ".join(self.z_order_columns)
        return f"CLUSTERING BY ({cols})"


# ========================================
# BRONZE LAYER CONFIG (Current)
# ========================================

BRONZE_CONFIG = IcebergConfig(
    # Bronze = raw data, high velocity streaming
    # Keep SHORT history for cost optimization
    metadata_max_age_days=7,  # 1 week (balance: audit trail vs storage cost)
    metadata_previous_versions_max=100,  # keep ~100 metadata file versions
    
    # Tuning: smaller files for streaming (10-sec batches)
    # More files but better for compaction
    target_file_size_bytes=134217728,  # 128 MB (2x smaller than default)
    compression_codec="snappy",
    
    partition_spec="days(event_time)",
    
    # Use hash distribution to allow multiple writers per partition
    distribution_mode="hash",
    
    # No Z-order for Bronze (raw data)
    z_order_columns=[],
    
    # Position deletes for flexibility
    delete_mode="position-deletes",
)