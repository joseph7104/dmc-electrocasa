def schema_ventas_vendedor() -> str:
    schema = """
        anho INT NOT NULL,
        mes INT NOT NULL,
        vendedor_id STRING NOT NULL,
        nombre STRING,
        sucursal_id STRING,
        sucursal STRING,
        nro_ventas BIGINT,
        cantidad BIGINT,
        monto_total DECIMAL(22, 2)
    """
    return schema