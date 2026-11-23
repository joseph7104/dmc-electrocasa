def schema_ventas_netas() -> str:
    schema = """
        anho INT NOT NULL,
        mes INT NOT NULL,
        sucursal_id STRING NOT NULL,
        nombre_sucursal STRING,
        ventas DECIMAL(22, 2),
        devoluciones DECIMAL(22, 2),
        ventas_netas DECIMAL(23, 2),
        cantidad_ventas BIGINT,
        ticket_promedio DECIMAL(23, 2)
    """
    return schema