def schema_ventas() -> str:
    schema = """
        venta_id STRING NOT NULL,
        sucursal_id STRING,
        producto_id STRING,
        vendedor_id STRING,
        cantidad INT,
        monto_total DECIMAL(12, 2),
        metodo_pago STRING,
        fecha_venta TIMESTAMP,
        canal STRING,
        source_file STRING,
        source_path STRING,
        fecha_snapshot TIMESTAMP,
        ingest_at TIMESTAMP,
        processed_at TIMESTAMP
    """
    return schema