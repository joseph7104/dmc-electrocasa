from pyspark import pipelines as dp
from pyspark.sql.functions import initcap, trim, col, when, lower, lit, regexp_extract, to_timestamp, expr, current_timestamp, upper, translate, regexp_replace, coalesce, to_date
from pyspark.sql.types import LongType, StringType, DateType, DecimalType, BooleanType, TimestampType
from datetime import datetime
from src.electrocasa.schemas.silver.catalogo import schema_catalogo
from src.electrocasa.utils.utils import parse_fecha_registro_safe

catalog = spark.conf.get("bundle.catalog")  
schema_bronze = spark.conf.get("bundle.schema_bronze")
schema_silver = spark.conf.get("bundle.schema_silver")

valid_expects = {
    "warning_price": "precio_lista > 0 AND precio_lista IS NOT NULL",
    "warning_idProducto": "producto_id IS NOT NULL"
}

@dp.temporary_view(
    name="view_catalogo",
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
        spark.readStream.table(f"{catalog}.{schema_bronze}.catalogo")
        .withColumn("producto_id", trim(col("producto_id")))
        .withColumn("nombre_producto", trim(col("nombre_producto")))
        .withColumn(
            "categoria",
            when(cat_norm.isin("LINEA_BLANCA", "COCINA", "ELECTRONICA", "CLIMATIZACION", "ENTRETENIMIENTO"), cat_norm)
            .otherwise("OTROS")
        )
        .withColumn("marca", initcap(lower(trim(col("marca")))))
        .withColumn("precio_norm", trim(regexp_replace(col("precio_lista"), r"[S/$\s]", "")))
        .withColumn(
            "precio_norm",
            when(
                col("precio_norm").contains(","),
                regexp_replace(regexp_replace(col("precio_norm"), r"\.", ""), ",", ".")
            ).otherwise(col("precio_norm"))
        )
        .withColumn("precio_lista", expr("try_cast(precio_norm AS DECIMAL(12, 2))"))
        .withColumn(
            "activo",
            when(lower(trim(col("activo"))).isin("true", "1", "si", "sí", "s"), True)
            .when(lower(trim(col("activo"))).isin("false", "0", "no", "n"), False)
            .otherwise(None)
        )
        .select(
            col("producto_id").cast(StringType()),
            col("nombre_producto").cast(StringType()),
            col("categoria").cast(StringType()),
            col("marca").cast(StringType()),
            col("precio_lista").cast(DecimalType(12, 2)),
            col("activo").cast(BooleanType()),
            coalesce(fecha_archivo, to_date(col("ingest_at"))).cast(DateType()).alias("fecha_snapshot"),
            col("file_metadata.file_name").cast(StringType()).alias("source_file"),
            col("ingest_at").cast(TimestampType()),
            current_timestamp().alias("processed_at")
        )
    )

    return df

dp.create_streaming_table(
    name=f"{catalog}.{schema_silver}.catalogo",
    comment="Estado actual de clientes VÁLIDOS (SCD Tipo 1)",
    schema=schema_catalogo()
)

dp.create_auto_cdc_flow(
    target=f"{catalog}.{schema_silver}.catalogo",
    source="view_catalogo",
    keys=["producto_id"],
    sequence_by="fecha_snapshot",
    column_list=[
        "producto_id",
        "nombre_producto",
        "categoria",
        "marca",
        "precio_lista",
        "activo",
        "fecha_snapshot",
        "source_file",
        "ingest_at",
        "processed_at"
    ],
    stored_as_scd_type=1,
    name="catalogo_cdc_flow"
)