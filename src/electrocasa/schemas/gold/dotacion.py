def schema_dotacion_empleados() -> str:
    schema = """
        anho INT NOT NULL,
        mes INT NOT NULL,
        ultimo_dia_mes DATE NOT NULL,
        sucursal_id STRING NOT NULL,
        nombre_sucursal STRING,
        cantidad_empleados_activos BIGINT
    """
    return schema