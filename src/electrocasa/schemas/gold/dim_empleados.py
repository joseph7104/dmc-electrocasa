catalog = spark.conf.get("bundle.catalog")
schema_silver = spark.conf.get("bundle.schema_silver")

def schema_dim_empleado() -> str:
    schema = f"""
        id_empleado STRING,
        nombre STRING,
        dni STRING MASK {catalog}.{schema_silver}.mask_dni,
        salario DECIMAL(12, 2) MASK {catalog}.{schema_silver}.mask_salario,
        cargo STRING
    """
    return schema