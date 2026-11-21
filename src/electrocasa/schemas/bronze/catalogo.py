def schema_catalogo():
    schema = """
            producto_id STRING,
            nombre_producto STRING,
            categoria STRING,
            marca STRING,
            precio_lista STRING
            activo STRING
           """
    return schema