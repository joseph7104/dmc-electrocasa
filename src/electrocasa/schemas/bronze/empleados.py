def schema_empleados():
    schema = """
            id_empleado STRING,
            nombre STRING,
            dni STRING,
            email STRING,
            salario STRING,
            sucursal_id STRING,
            cargo STRING,
            tipo_evento STRING,
            fecha_evento STRING,
            fecha_registro STRING
           """
    return schema