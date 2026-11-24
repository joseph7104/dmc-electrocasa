def schema_sucursales() -> str:
    schema = """
        sucursal_id STRING NOT NULL,
        nombre_sucursal STRING,
        distrito STRING,
        ciudad STRING,
        region STRING,
        processed_at TIMESTAMP
    """
    return schema