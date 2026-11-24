from pyspark import pipelines as dp
from pyspark.sql.functions import trim, col, current_timestamp, upper, inticap
from pyspark.sql.types import StringType, TimestampType
from src.electrocasa.schemas.silver.sucursales import schema_sucursales

catalog = spark.conf.get("bundle.catalog")
schema_bronze = spark.conf.get("bundle.schema_bronze")
schema_silver = spark.conf.get("bundle.schema_silver")

@dp.materialized_view(
    name=f"{catalog}.{schema_silver}.sucursales",
    comment="Tabla maestra limpia de sucursales",
    schema=schema_sucursales()
)
@dp.expect_all_or_drop({"warning_sucursal_id": "sucursal_id IS NOT NULL"})
def silver_sucursales():
    return (
        spark.read.table(f"{catalog}.{schema_bronze}.sucursales")
        .withColumn("sucursal_id", trim(col("sucursal_id")))
        .withColumn("nombre_sucursal", trim(col("nombre_sucursal")))
        .withColumn("distrito", trim(col("distrito")))
        .withColumn("ciudad", initcap(trim(col("ciudad"))))
        .withColumn("region", trim(col("region")))
        .select(
            col("sucursal_id").cast(StringType()),
            col("nombre_sucursal").cast(StringType()),
            col("distrito").cast(StringType()),
            col("ciudad").cast(StringType()),
            col("region").cast(StringType()),
            current_timestamp().alias("processed_at")
        )
    )