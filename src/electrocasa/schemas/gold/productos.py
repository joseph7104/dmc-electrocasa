def schema_productos_cantidad() -> str:
    schema = """
        producto_id STRING NOT NULL,
        nombre_producto STRING,
        cantidad BIGINT
    """
    return schema


def schema_productos_devolucion() -> str:
    schema = """
        producto_id STRING NOT NULL,
        nombre_producto STRING,
        ratio DOUBLE
    """
    return schema