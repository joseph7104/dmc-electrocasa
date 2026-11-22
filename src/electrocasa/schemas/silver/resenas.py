def schema_resenas() -> str:
    schema = """
        resena_id STRING NOT NULL,
        producto_id STRING,
        cliente_id STRING,
        calificacion INT,
        comentario STRING,
        tags ARRAY<STRING>,
        respuesta_autor STRING,
        respuesta_texto STRING,
        respuesta_fecha TIMESTAMP,
        fecha_resena TIMESTAMP,
        fecha_snapshot TIMESTAMP,
        source_file STRING,
        source_path STRING,
        ingest_at TIMESTAMP,
        processed_at TIMESTAMP
    """
    return schema