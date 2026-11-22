def schema_devoluciones() -> str:
    schema = """
        devolucion_id STRING NOT NULL,
        venta_id STRING NOT NULL,
        sucursal_id STRING,
        producto_id STRING,
        motivo STRING,
        monto_reembolso DECIMAL(12, 2),
        fecha_devolucion DATE,
        source_file STRING,
        ingest_at TIMESTAMP,
        processed_at TIMESTAMP
    """
    return schema