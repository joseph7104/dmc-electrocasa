from pyspark import pipelines as dp
from src.electrocasa.schemas.gold.ventas_vendedor import schema_ventas_vendedor

catalog = spark.conf.get("bundle.catalog")


@dp.materialized_view(
    name=f"{catalog}.gold.ventas_vendedor",
    comment="Pregunta 4: Ventas en tienda física por vendedor, mes y sucursal vigente según historial SCD Type 2 (marzo-octubre 2026).",
    schema=schema_ventas_vendedor()
)
def ventas_vendedor():
    return spark.sql(f"""
        WITH ventas_base AS(
            SELECT venta_id, vendedor_id, cantidad, monto_total, fecha_venta FROM electrocasa_dev.silver.ventas
            WHERE vendedor_id IS NOT NULL AND canal = 'TIENDA_FISICA'
        ),
        empleados as(
            SELECT ep.id_empleado, ep.nombre, ep.sucursal_id, s.nombre_sucursal, ep.__START_AT, ep.__END_AT FROM electrocasa_dev.silver.empleados ep
            INNER JOIN electrocasa_dev.silver.sucursales s ON s.sucursal_id = ep.sucursal_id
        ),
        vista_cruzada AS(
            SELECT EXTRACT(YEAR FROM vb.fecha_venta) anho, EXTRACT(MONTH FROM vb.fecha_venta) mes,vb.venta_id, vb.vendedor_id,
            CASE
                WHEN ep.nombre IS NULL THEN 'SIN_ASIGNAR'
                ELSE ep.nombre
            END nombre
            , vb.cantidad, vb.monto_total, ep.sucursal_id, 
            CASE
                WHEN ep.nombre_sucursal IS NULL THEN 'SIN_ASIGNAR'
                ELSE ep.nombre_sucursal
            END sucursal
            FROM ventas_base vb
            LEFT JOIN empleados ep ON vb.vendedor_id = ep.id_empleado AND vb.fecha_venta >= ep.__START_AT AND (vb.fecha_venta < ep.__END_AT OR ep.__END_AT IS NULL)
        ),
        vista_resultado AS(
            SELECT 
            anho, 
            mes,
            vendedor_id,
            nombre,
            sucursal_id,
            sucursal,
            COUNT(venta_id) nro_ventas,
            SUM(cantidad) cantidad,
            SUM(monto_total) monto_total
            FROM vista_cruzada
            GROUP BY 1, 2, 3, 4, 5, 6
            ORDER BY 1, 2, 3, 4, 5, 6
        )
        SELECT * FROM vista_resultado 
    """)