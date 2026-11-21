from pyspark.sql.functions import col, current_timestamp
from pyspark import pipelines as dp
from src.electrocasa.schemas.bronze.resenas import schema_resenas

# Leídos desde configuration (resources/project_sdp_etl.pipeline.yml), no
# hardcodeados: catalog y landing_path cambian según el target (dev/test).
catalog = spark.conf.get("bundle.catalog")
schema_bronze = spark.conf.get("bundle.schema_bronze")
landing_path = spark.conf.get("bundle.landing_path")


@dp.table(
    name=f"{catalog}.{schema_bronze}.resenas",
    comment="Tabla Bronze resenas",
    table_properties={
        "quality": "bronze",
        "pipelines.reset.allowed": "false",
        "delta.appendOnly": "true",
    },
)
def bronze_table():
    df_reader = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(schema_resenas())
        .load(f'{landing_path}/resenas')
        .withColumn("ingest_at", current_timestamp())
        .withColumn("file_metadata", col("_metadata"))
    )

    return df_reader