def schema_sucursales() -> str:
    schema = """
        sucursal_id STRING NOT NULL,
        nombre_sucursal STRING,
        distrito STRING,
        ciudad STRING,
        region STRING,
        source_file STRING,
        source_path STRING,
        ingest_at TIMESTAMP,
        processed_at TIMESTAMP
    """
    return schema