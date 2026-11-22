def schema_catalogo() -> str:
    schema = """
        producto_id STRING NOT NULL,
        nombre_producto STRING,
        categoria STRING,
        marca STRING,
        precio_lista DECIMAL(12, 2),
        activo BOOLEAN,
        fecha_snapshot DATE,
        source_file STRING,
        ingest_at TIMESTAMP,
        processed_at TIMESTAMP
    """
    return schema