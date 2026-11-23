def schema_resenas() -> str:
    schema = """
        anho INT NOT NULL,
        mes INT NOT NULL,
        categoria STRING NOT NULL,
        categoria_calificacion STRING,
        porcentaje DECIMAL(10, 2)
    """
    return schema