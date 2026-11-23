def schema_despacho() -> str:
    schema = """
        courier STRING NOT NULL,
        tiempo_promedio_horas DOUBLE,
        porcentaje_devueltos DECIMAL(10, 2),
        total_envios_finalizados BIGINT,
        total_envios_devueltos BIGINT
    """
    return schema