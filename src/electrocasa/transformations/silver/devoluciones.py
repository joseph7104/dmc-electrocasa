from pyspark import pipelines as dp
from pyspark.sql.functions import initcap, trim, col, when, lower, lit, regexp_extract, to_timestamp, expr, current_timestamp, upper, translate, regexp_replace, coalesce, to_date
from pyspark.sql.types import LongType, StringType, DateType, DecimalType, BooleanType, TimestampType, IntegerType, ArrayType
from datetime import datetime
from src.electrocasa.schemas.silver.devoluciones import schema_devoluciones
from src.electrocasa.utils.utils import parse_fecha_registro_safe

catalog = spark.conf.get("bundle.catalog")  
schema_bronze = spark.conf.get("bundle.schema_bronze")
schema_silver = spark.conf.get("bundle.schema_silver")



drop_expects = {
    "warning_montoReembolso" : "monto_reembolso > 0",
    "warning_idReembolso": "devolucion_id IS NOT NULL"
}


@dp.temporary_view(
    name="view_devoluciones",
    comment="Devoluciones limpio y correcto"
)
@dp.expect_all_or_drop(drop_expects)

def staging_catalogo():
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
        spark.readStream.table(f"{catalog}.{schema_bronze}.devoluciones")
        .withColumn("devolucion_id", trim(col("devolucion_id")))
        .withColumn("venta_id", trim(col("venta_id")))
        .withColumn("producto_id", trim(col("producto_id")))
        .withColumn("sucursal_id", trim(col("sucursal_id")))
        .withColumn("motivo", trim(col("motivo")))
        .withColumn("precio_norm", trim(regexp_replace(col("monto_reembolso"), r"[S/$\s]", "")))
        .withColumn(
            "precio_norm",
            when(
                col("precio_norm").contains(","),
                regexp_replace(regexp_replace(col("precio_norm"), r"\.", ""), ",", ".")
            ).otherwise(col("precio_norm"))
        )
        .withColumn("monto_reembolso", expr("try_cast(precio_norm AS DECIMAL(12, 2))"))
        .withColumn('fecha_devolucion', parse_fecha_registro_safe(col('fecha_devolucion')))
        .select(
            col("devolucion_id").cast(StringType()),
            col("venta_id").cast(StringType()),
            col("producto_id").cast(StringType()),
            col("sucursal_id").cast(StringType()),
            col("motivo").cast(StringType()),
            col("monto_reembolso").cast(DecimalType(12,2)),
            col("fecha_devolucion").cast(TimestampType()),
            col("file_metadata.file_name").alias("source_file"),
            col("file_metadata.file_path").alias("source_path"),
            col("file_metadata.file_modification_time").cast(TimestampType()).alias("fecha_snapshot"),
            col("ingest_at").cast(TimestampType()),
            current_timestamp().alias("processed_at")
        )
    )

    return df

dp.create_streaming_table(
    name=f"{catalog}.{schema_silver}.devoluciones_dedup",
    comment="Estado actual de devoluciones VÁLIDOS (SCD Tipo 1)",
    schema=schema_devoluciones()
)

dp.create_auto_cdc_flow(
    target=f"{catalog}.{schema_silver}.devoluciones_dedup",
    source="view_devoluciones",
    keys=["devolucion_id"],
    sequence_by="fecha_snapshot",
    stored_as_scd_type=1,
    name="devoluciones_cdc_flow"
)




drop_expects_negocio = {
    "drop_venta_inexistente": "monto_venta IS NOT NULL",
    "drop_monto_excede_venta": "monto_reembolso <= monto_venta"
}

@dp.materialized_view(
    name=f"{catalog}.{schema_silver}.devoluciones",
    comment="Devoluciones deduplicadas y validadas contra el monto de venta"
)
@dp.expect_all_or_drop(drop_expects_negocio)

def silver_devoluciones_validadas():
    df_dev = spark.read.table(f"{catalog}.{schema_silver}.devoluciones_dedup")
    
    df_ventas = (
        spark.read.table(f"{catalog}.{schema_silver}.ventas")
        .select(
            col("venta_id"),
            col("monto_total").alias("monto_venta")
        )
    )

    return (
        df_dev.join(df_ventas, on="venta_id", how="left")
        .select(
            df_dev["*"],
            col("monto_venta") # Se mantiene para evaluar el expect (puedes quitarla si haces el expect en una vista intermedia)
        )
    )
