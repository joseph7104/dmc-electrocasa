def schema_devoluciones():
    schema = """
            devolucion_id STRING,
            venta_id STRING,
            sucursal_id STRING,
            producto_id STRING,
            motivo STRING,
            monto_reembolso STRING,
            fecha_devolucion STRING
           """
    return schema