from pyspark import pipelines as dp
from pyspark.sql.functions import initcap, trim, col, when, lower, lit, regexp_extract, to_timestamp, expr, current_timestamp, upper, translate, regexp_replace, coalesce, to_date
from pyspark.sql.types import LongType, StringType, DateType, DecimalType, BooleanType, TimestampType, IntegerType, ArrayType
from datetime import datetime
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
    comment="Catálogo limpio y correcto"
)
@dp.expect_all_or_drop(valid_expects)

def staging_catalogo():

    cat_norm = translate(
        upper(regexp_replace(trim(col("categoria")), r"[\s-]+", "_")),
        "ÁÉÍÓÚ",
        "AEIOU"
    )

    fecha_archivo = to_date(
        regexp_extract(col("file_metadata.file_name"), r"(\d{4}-\d{2}-\d{2})", 1)
    )

    today = datetime.now().date()

    df = (
        spark.readStream.table(f"{catalog}.{schema_bronze}.resenas")
        .withColumn("resena_id", trim(col("resena_id")))
        .withColumn("producto_id", trim(col("producto_id")))
        .withColumn("cliente_id", trim(col("cliente_id")))
        .withColumn("comentario", trim(col("comentario")))
        .withColumn('fecha_resena', parse_fecha_registro_safe(col('fecha_resena')))
        .select(
            col("resena_id").cast(StringType()),
            col("producto_id").cast(StringType()),
            col("cliente_id").cast(StringType()),
            col("calificacion").cast(IntegerType()),
            col("comentario").cast(StringType()),
            col("tags").cast(ArrayType(StringType())),
            col('respuesta.autor').cast(StringType()).alias('autor'),
            col('respuesta.texto').cast(StringType()).alias('respuesta'),
            col('respuesta.fecha').cast(TimestampType()).alias('fecha_comentario'),
            col("file_metadata.file_name").alias("source_file"),
            col("file_metadata.file_path").alias("source_path"),
            col("file_metadata.file_modification_time").cast(TimestampType()).alias("fecha_snapshot"),
            col("fecha_resena").cast(TimestampType())

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