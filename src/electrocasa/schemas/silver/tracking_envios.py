def schema_tracking_envios() -> str:
    schema = """
        evento_id INT NOT NULL,
        tracking_id STRING,
        venta_id STRING,
        courier STRING,
        estado_entrega STRING,
        sucursal_origen STRING,
        fecha_actualizacion DATE,
        fecha_modificacion TIMESTAMP,
        processed_at TIMESTAMP
    """
    return schema