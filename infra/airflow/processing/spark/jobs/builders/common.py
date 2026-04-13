"""Common utilities and imports for builders."""

from __future__ import annotations

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

from silver_transform_utils import EXCHANGE_RATES


def micros_to_timestamp(col_name: str) -> F.Column:
    """Convert Debezium microsecond epoch to Spark TIMESTAMP."""
    return F.when(
        F.col(col_name).isNull() | (F.col(col_name) <= F.lit(0)),
        F.lit(None).cast("timestamp"),
    ).otherwise(F.expr(f"timestamp_micros({col_name})"))


def surrogate_key(*columns: F.Column) -> F.Column:
    """Build deterministic surrogate key from business key + source timestamp."""
    normalized = [F.coalesce(col.cast("string"), F.lit("")) for col in columns]
    return F.sha2(F.concat_ws("||", *normalized), 256)


def get_exchange_rates_df(spark: SparkSession):
    """Create broadcast DataFrame from hardcoded exchange rates array."""
    schema = StructType([
        StructField("source_currency", StringType(), False),
        StructField("target_currency", StringType(), False),
        StructField("exchange_rate", DoubleType(), False),
    ])
    
    df = spark.createDataFrame(EXCHANGE_RATES, schema=schema)
    return df.select(
        F.col("source_currency"),
        F.col("exchange_rate"),
    )
