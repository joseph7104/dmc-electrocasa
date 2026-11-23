from pyspark.sql import SparkSession

def schema_empleado() -> str:
    spark = SparkSession.getActiveSession()
    catalog = spark.conf.get("bundle.catalog")
    schema_silver = spark.conf.get("bundle.schema_silver")
    schema = f"""
        id_empleado STRING NOT NULL,
        nombre STRING,
        dni STRING MASK {catalog}.{schema_silver}.mask_dni,
        email STRING,
        salario DECIMAL(12, 2) MASK {catalog}.{schema_silver}.mask_salario,
        sucursal_id STRING, 
        cargo STRING,
        tipo_evento STRING,
        fecha_evento TIMESTAMP,
        fecha_registro TIMESTAMP,
        source_file STRING,
        ingest_at TIMESTAMP,
        processed_at TIMESTAMP,
        __START_AT TIMESTAMP,
        __END_AT TIMESTAMP
    """
    return schema