from pyspark import pipelines as dp
from pyspark.sql.functions import (
    trim, col, when, expr, current_timestamp,
    upper, translate, regexp_replace
)
from pyspark.sql.types import (
    StringType, DecimalType, TimestampType, IntegerType
)
from src.electrocasa.schemas.silver.ventas import schema_ventas
from src.electrocasa.utils.utils import parse_fecha_registro_safe

catalog = spark.conf.get("bundle.catalog")  
schema_bronze = spark.conf.get("bundle.schema_bronze")
schema_silver = spark.conf.get("bundle.schema_silver")

drop_expects = {
    "drop_idVenta_nulo": "venta_id IS NOT NULL AND trim(venta_id) != ''",
    "drop_monto_invalido": "monto_total > 0"
}

warn_expects = {
    "warning_vendedor": "vendedor_id IS NOT NULL"
}

@dp.temporary_view(
    name="view_ventas",
    comment="Ventas limpias y validadas"
)
@dp.expect_all_or_drop(drop_expects)
@dp.expect_all(warn_expects)
def staging_ventas():

    cat_norm = translate(
        upper(regexp_replace(trim(col("metodo_pago")), r"[\s-]+", "_")),
        "ÁÉÍÓÚ",
        "AEIOU"
    )
    cat_canal = translate(
        upper(regexp_replace(trim(col("canal")), r"[\s-]+", "_")),
        "ÁÉÍÓÚ",
        "AEIOU"
    )

    df = (
        spark.readStream.table(f"{catalog}.{schema_bronze}.ventas")
        .withColumn("venta_id", trim(col("venta_id")))
        .withColumn("sucursal_id", trim(col("sucursal_id")))
        .withColumn("producto_id", trim(col("producto_id")))
        .withColumn("vendedor_id", trim(col("vendedor_id")))
        .withColumn("cantidad", expr("try_cast(trim(cantidad) AS INT)"))
        .withColumn("precio_norm", trim(regexp_replace(col("monto_total"), r"[S/$\s]", "")))
        .withColumn(
            "precio_norm",
            when(
                col("precio_norm").contains(","),
                regexp_replace(regexp_replace(col("precio_norm"), r"\.", ""), ",", ".")
            ).otherwise(col("precio_norm"))
        )
        .withColumn("monto_total", expr("try_cast(precio_norm AS DECIMAL(12, 2))"))
        .withColumn("metodo_pago", cat_norm)
        .withColumn("fecha_venta", parse_fecha_registro_safe(col("fecha_venta")))
        .withColumn("canal", cat_canal)
        .select(
            col("venta_id").cast(StringType()),
            col("sucursal_id").cast(StringType()),
            col("producto_id").cast(StringType()),
            col("vendedor_id").cast(StringType()),
            col("cantidad").cast(IntegerType()),
            col("monto_total").cast(DecimalType(12, 2)),
            col("metodo_pago").cast(StringType()),
            col("fecha_venta").cast(TimestampType()),
            col("canal").cast(StringType()),
            col("file_metadata.file_name").cast(StringType()).alias("source_file"),
            col("file_metadata.file_path").cast(StringType()).alias("source_path"),
            col("file_metadata.file_modification_time").cast(TimestampType()).alias("fecha_snapshot"),
            col("ingest_at").cast(TimestampType()),
            current_timestamp().alias("processed_at")
        )
    )

    return df

dp.create_streaming_table(
    name=f"{catalog}.{schema_silver}.ventas",
    comment="Estado actual de ventas VÁLIDAS (SCD Tipo 1)",
    schema=schema_ventas()
)

dp.create_auto_cdc_flow(
    target=f"{catalog}.{schema_silver}.ventas",
    source="view_ventas",
    keys=["venta_id"],
    sequence_by="fecha_snapshot",
    stored_as_scd_type=1,
    name="ventas_cdc_flow"
)