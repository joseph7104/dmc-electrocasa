def schema_devoluciones() -> str:
    schema = """
        devolucion_id STRING NOT NULL,
        venta_id STRING,
        producto_id STRING,
        sucursal_id STRING,
        motivo STRING,
        monto_reembolso DECIMAL(12, 2),
        fecha_devolucion TIMESTAMP,
        source_file STRING,
        source_path STRING,
        fecha_snapshot TIMESTAMP,
        ingest_at TIMESTAMP,
        processed_at TIMESTAMP
    """
    return schema