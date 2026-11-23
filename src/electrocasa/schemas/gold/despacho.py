def schema_despacho() -> str:
    schema = """
        courier STRING,
        tiempo_promedio_horas DECIMAL(24, 2),
        porcentaje_devueltos DECIMAL(27, 2),
        total_envios_finalizados BIGINT,
        total_envios_devueltos BIGINT
    """
    return schema