def schema_ventas():
    schema = """
            venta_id STRING,
            sucursal_id STRING,
            producto_id STRING,
            vendedor_id STRING,
            cantidad STRING,
            monto_total STRING,
            metodo_pago STRING,
            fecha_venta STRING,
            canal STRING
           """
    return schema