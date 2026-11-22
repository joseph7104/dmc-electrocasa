def schema_empleado() -> str:
    schema = """
        id_empleado STRING NOT NULL,
        nombre STRING,
        dni STRING,
        email STRING,
        salario DECIMAL(12, 2),
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