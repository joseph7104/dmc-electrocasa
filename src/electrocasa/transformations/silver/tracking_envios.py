from pyspark import pipelines as dp
from pyspark.sql.functions import initcap, trim, col, when, lower, lit, regexp_extract, to_timestamp, expr, current_timestamp, upper, translate, regexp_replace, coalesce, to_date
from pyspark.sql.types import LongType, StringType, DateType, DecimalType, BooleanType, TimestampType, IntegerType, ArrayType
from datetime import datetime
from src.electrocasa.schemas.silver.tracking_envios import schema_tracking_envios
from src.electrocasa.utils.utils import parse_fecha_registro_safe

catalog = spark.conf.get("bundle.catalog")  
schema_bronze = spark.conf.get("bundle.schema_bronze")
schema_silver = spark.conf.get("bundle.schema_silver")

drop_expects = {
    "warning_estados": "estado_entrega IN ('PENDIENTE','EN_TRANSITO','ENTREGADO','DEVUELTO')",
    "warning_idEvento":"evento_id IS NOT NULL"
}



@dp.temporary_view(
    name="view_tracking_envios",
    comment="tracking_envios limpio y correcto"
)
@dp.expect_all_or_drop(drop_expects)

def staging_catalogo():

    cat_norm = translate(
        upper(regexp_replace(trim(col("estado_entrega")), r"[\s-]+", "_")),
        "ÁÉÍÓÚ",
        "AEIOU"
    )
    cat_canal = translate(
        upper(regexp_replace(trim(col("canal")), r"[\s-]+", "_")),
        "ÁÉÍÓÚ",
        "AEIOU"
    )

    df = (
        spark.readStream.table(f"{catalog}.{schema_bronze}.tracking_envios")
        .withColumn("tracking_id", trim(col("tracking_id")))
        .withColumn("venta_id", trim(col("venta_id")))
        .withColumn("courier", trim(col("courier")))
        .withColumn("estado_entrega", cat_norm)
        .withColumn("sucursal_origen", trim(col("sucursal_origen")))
        .withColumn(
            "seq_cdc",
            coalesce(
                col("fecha_modificacion").cast(TimestampType()),
                col("fecha_actualizacion").cast(TimestampType())
            )
        )
        #.withColumn("fecha_actualizacion", parse_fecha_registro_safe(col('fecha_actualizacion')))
        #.withColumn("fecha_modificacion", parse_fecha_registro_safe(col('fecha_modificacion')))
        .select(
            col("evento_id").cast(IntegerType()),
            col("tracking_id").cast(StringType()),
            col("venta_id").cast(StringType()),
            col("courier").cast(StringType()),
            col("estado_entrega").cast(StringType()),
            col("sucursal_origen").cast(StringType()),
            col("fecha_actualizacion").cast(DateType()),
            col("fecha_modificacion").cast(TimestampType()),
            col("seq_cdc").cast(TimestampType()),
            current_timestamp().alias("processed_at")
        )
    )

    return df

dp.create_streaming_table(
    name=f"{catalog}.{schema_silver}.tracking_envios",
    comment="Estado actual de tracking VÁLIDOS (SCD Tipo 1)",
    schema=schema_tracking_envios()
)

dp.create_auto_cdc_flow(
    target=f"{catalog}.{schema_silver}.tracking_envios",
    source="view_tracking_envios",
    keys=["evento_id"],
    sequence_by="seq_cdc",
    exclude_columns=["seq_cdc"],
    stored_as_scd_type=1,
    name="tracking_envios_cdc_flow"
)
