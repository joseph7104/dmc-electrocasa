from pyspark.sql import SparkSession


def schema_dim_empleado() -> str:
    spark = SparkSession.getActiveSession()
    assert spark is not None
    catalog = spark.conf.get("bundle.catalog")
    schema_silver = spark.conf.get("bundle.schema_silver")

    schema = f"""
        id_empleado STRING NOT NULL,
        nombre STRING,
        dni STRING MASK {catalog}.{schema_silver}.mask_dni,
        salario DECIMAL(12, 2) MASK {catalog}.{schema_silver}.mask_salario,
        cargo STRING
    """
    return schema