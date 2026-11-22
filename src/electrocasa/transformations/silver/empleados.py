from pyspark import pipelines as dp
from pyspark.sql.functions import initcap, trim, col, when, lower, lit, regexp_extract, to_timestamp, expr, current_timestamp, upper, translate, regexp_replace, coalesce, to_date
from pyspark.sql.types import LongType, StringType, DateType, DecimalType, BooleanType, TimestampType
from datetime import datetime
from src.electrocasa.schemas.silver.empleados import schema_empleado
from src.electrocasa.utils.utils import parse_fecha_registro_safe

catalog = spark.conf.get("bundle.catalog")  
schema_bronze = spark.conf.get("bundle.schema_bronze")
schema_silver = spark.conf.get("bundle.schema_silver")

valid_expects = {
    "warning_empleado": "id_empleado IS NOT NULL",
    "warning_fecha_evento": "fecha_evento IS NOT NULL"
}

@dp.temporary_view(
    name="view_empleado",
    comment="Empleado limpio y correcto"
)
@dp.expect_all_or_drop(valid_expects)

def staging_empleado():

    cat_norm = translate(
        upper(regexp_replace(trim(col("tipo_evento")), r"[\s-]+", "_")),
        "ÁÉÍÓÚ",
        "AEIOU"
    )

    df = (
        spark.readStream.table(f"{catalog}.{schema_bronze}.empleado")
        .withColumn("id_empleado", trim(col("id_empleado")))
        .withColumn("nombre", trim(col("nombre")))
        .withColumn("dni", trim(col("dni")))
        .withColumn("email", trim(col("email")))
        .withColumn("precio_norm", trim(regexp_replace(col("salario"), r"[S/$\s]", "")))
        .withColumn(
            "precio_norm",
            when(
                col("precio_norm").contains(","),
                regexp_replace(regexp_replace(col("precio_norm"), r"\.", ""), ",", ".")
            ).otherwise(col("precio_norm"))
        )
        .withColumn("salario", expr("try_cast(precio_norm AS DECIMAL(12, 2))"))
        .withColumn("cargo", trim(col("cargo")))
        .withColumn("sucursal_id", trim(col("sucursal_id")))
        .withColumn("tipo_evento", cat_norm)
        .withColumn("fecha_evento", parse_fecha_registro_safe(col('fecha_evento')))
        .withColumn("fecha_registro", parse_fecha_registro_safe(col('fecha_registro')))
        .select(
            col("id_empleado").cast(StringType()),
            col("nombre").cast(StringType()),
            col("dni").cast(StringType()),
            col("email").cast(StringType()),
            col("salario").cast(DecimalType(12, 2)),
            col("cargo").cast(StringType()),
            col("tipo_evento").cast(StringType()),
            col("fecha_evento").cast(TimestampType()),
            col("fecha_registro").cast(TimestampType()),
            col("file_metadata.file_name").cast(StringType()).alias("source_file"),
            col("ingest_at").cast(TimestampType()),
            current_timestamp().alias("processed_at")
        )
    )

    return df

dp.create_streaming_table(
    name=f"{catalog}.{schema_silver}.empleado",
    comment="Estado actual de Empleados VÁLIDOS (SCD Tipo 2)",
    schema=schema_empleado()
)

dp.create_auto_cdc_flow(
    target=f"{catalog}.{schema_silver}.empleado",
    source="view_empleado",
    keys=["id_empleado"],
    sequence_by="fecha_evento",
    apply_as_deletes="tipo_evento = 'BAJA'",
    stored_as_scd_type=2,
    track_history_except_column_list=[
        "tipo_evento",
        "fecha_evento",
        "fecha_registro",
        "source_file",
        "ingest_at",
        "processed_at"
    ],
    name="empleados_cdc_flow"
)