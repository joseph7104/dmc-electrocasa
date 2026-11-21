def schema_resenas() -> str:
    return """
        resena_id STRING,
        producto_id STRING,
        cliente_id STRING,
        calificacion STRING,
        comentario STRING,
        tags ARRAY<STRING>,
        respuestas ARRAY<STRING>,
        fecha_resena STRING,
        metadata STRUCT<
            origen: STRING,
            app: STRUCT<version: STRING, so: STRING>,
            kiosko_id: STRING,
            sucursal_id: STRING
        >
    """