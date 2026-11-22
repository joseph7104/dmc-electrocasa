from pyspark import pipelines as dp
from pyspark.sql.functions import trim, col, lit, current_timestamp, explode_outer
from pyspark.sql.types import StringType, TimestampType, IntegerType, ArrayType
from src.electrocasa.schemas.silver.resenas import schema_resenas
from src.electrocasa.utils.utils import parse_fecha_registro_safe

catalog = spark.conf.get("bundle.catalog")  
schema_bronze = spark.conf.get("bundle.schema_bronze")
schema_silver = spark.conf.get("bundle.schema_silver")

valid_expects = {
    "warning_calificacion": "calificacion > 0 AND calificacion <= 5",
    "warning_idResena": "resena_id IS NOT NULL"
}

@dp.temporary_view(
    name="view_resenas",
    comment="Reseñas limpias y validadas"
)
@dp.expect_all_or_drop(valid_expects)
def staging_resenas():
    df = (
        spark.readStream.table(f"{catalog}.{schema_bronze}.resenas")
        .withColumn("resena_id", trim(col("resena_id")))
        .withColumn("producto_id", trim(col("producto_id")))
        .withColumn("cliente_id", trim(col("cliente_id")))
        .withColumn("comentario", trim(col("comentario")))
        .withColumn("fecha_resena", parse_fecha_registro_safe(col("fecha_resena")))
        .select(
            col("resena_id").cast(StringType()),
            col("producto_id").cast(StringType()),
            col("cliente_id").cast(StringType()),
            col("calificacion").cast(IntegerType()),
            col("comentario").cast(StringType()),
            col("tags").cast(ArrayType(StringType())),
            lit(None).cast(StringType()).alias("respuesta_autor"),
            lit(None).cast(StringType()).alias("respuesta_texto"),
            lit(None).cast(TimestampType()).alias("respuesta_fecha"),
            col("fecha_resena").cast(TimestampType()),
            col("file_metadata.file_modification_time").cast(TimestampType()).alias("fecha_snapshot"),
            col("file_metadata.file_name").cast(StringType()).alias("source_file"),
            col("file_metadata.file_path").cast(StringType()).alias("source_path"),
            col("ingest_at").cast(TimestampType()),
            current_timestamp().alias("processed_at")
        )
    )

    return df

dp.create_streaming_table(
    name=f"{catalog}.{schema_silver}.resenas",
    comment="Estado actual de reseñas VÁLIDOS (SCD Tipo 1)",
    schema=schema_resenas()
)

dp.create_auto_cdc_flow(
    target=f"{catalog}.{schema_silver}.resenas",
    source="view_resenas",
    keys=["resena_id"],
    sequence_by="fecha_snapshot",
    stored_as_scd_type=1,
    name="resenas_cdc_flow"
)

@dp.materialized_view(
    name=f"{catalog}.{schema_silver}.resenas_tags",
    comment="Tags individuales explotados desde las reseñas vigentes"
)
def silver_resenas_tags():
    return (
        spark.read.table(f"{catalog}.{schema_silver}.resenas")
        .select(
            col("resena_id"),
            explode_outer(col("tags")).alias("tag")
        )
    )